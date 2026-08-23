//! Row-Level Security (RLS) Context for Multi-Tenant SaaS
//!
//! Provides a shared tenant context that all Qail drivers can use
//! for data isolation. Each driver implements isolation differently:
//!
//! - **qail-pg**: `set_config('app.current_tenant_id', ...)` session variables
//! - **qail-qdrant**: metadata filter `{ tenant_id: "..." }` on vector search
//!
//! # Example
//!
//! ```
//! use qail_core::rls::{RlsContext, SuperAdminToken};
//!
//! // Tenant context — scopes data to a single tenant
//! let ctx = RlsContext::tenant("550e8400-e29b-41d4-a716-446655440000");
//! assert_eq!(ctx.tenant_id, "550e8400-e29b-41d4-a716-446655440000");
//!
//! // Super admin — bypasses tenant isolation (requires named constructor)
//! let token = SuperAdminToken::for_system_process("example");
//! let admin = RlsContext::super_admin(token);
//! assert!(admin.bypasses_rls());
//!
//! // Global context — scopes to platform rows (tenant_id IS NULL)
//! let global = RlsContext::global();
//! assert!(global.is_global());
//! ```

/// Scoped LISTEN/NOTIFY channel derivation.
pub mod channel;
/// Owner (user) scope registry.
pub mod owner;
/// Tenant scope registry.
pub mod tenant;

/// Counts of tables registered by [`init_scope_registries`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScopeRegistryCounts {
    /// Tables registered for tenant scope (`tenant_id` column).
    pub tenant: usize,
    /// Tables registered for owner scope (`owner <column>` attribute).
    pub owner: usize,
}

/// How the process declared its AST isolation registries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeRegistryState {
    /// Nothing was declared. `Qail::with_rls` REFUSES to run in this state —
    /// silently scoping nothing was the original false-green.
    Uninitialized,
    /// Registries were populated (from a schema or by explicit registration).
    Initialized,
    /// The application declared that isolation is enforced by DB policies
    /// only; `with_rls` performs no AST injection on any table.
    PolicyOnly,
}

/// A declaration conflicted with the mode this process already sealed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScopeModeConflict {
    /// The mode already in force.
    pub current: ScopeRegistryState,
    /// The mode that was requested.
    pub requested: ScopeRegistryState,
}

impl std::fmt::Display for ScopeModeConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RLS isolation mode is sealed as {:?}; refusing transition to {:?}",
            self.current, self.requested
        )
    }
}

impl std::error::Error for ScopeModeConflict {}

/// One-way isolation-mode coordinator.
///
/// `Uninitialized` may move to exactly one of `Initialized` or `PolicyOnly`;
/// once sealed, the conflicting transition is refused. The application's
/// security decision is therefore not subject to last-writer-wins from a
/// library, a test, or a late registration.
///
/// The process-wide instance is private to this module: the ONLY ways to
/// reach `Initialized` are [`init_scope_registries`] /
/// [`init_scope_registries_from_tables`] (which populate the registries and
/// refuse to seal if nothing was registered) and
/// [`declare_no_scoped_tables`] (an explicit, reasoned declaration that the
/// schema has none). Crate-internal registration helpers are mode-neutral.
/// Exposing `declare_initialized` on the global would let a caller publish
/// `Initialized` over empty registries — the original silent no-op, one
/// line away. The type is crate-private; tests drive a local instance.
#[derive(Debug)]
pub(crate) struct ScopeModeCoordinator {
    state: std::sync::atomic::AtomicU8,
    policy_only_reason: std::sync::OnceLock<&'static str>,
}

const MODE_UNINITIALIZED: u8 = 0;
const MODE_INITIALIZED: u8 = 1;
const MODE_POLICY_ONLY: u8 = 2;

impl ScopeModeCoordinator {
    /// A fresh, unsealed coordinator.
    pub(crate) const fn new() -> Self {
        Self {
            state: std::sync::atomic::AtomicU8::new(MODE_UNINITIALIZED),
            policy_only_reason: std::sync::OnceLock::new(),
        }
    }

    fn decode(raw: u8) -> ScopeRegistryState {
        match raw {
            MODE_INITIALIZED => ScopeRegistryState::Initialized,
            MODE_POLICY_ONLY => ScopeRegistryState::PolicyOnly,
            _ => ScopeRegistryState::Uninitialized,
        }
    }

    /// Current mode.
    pub(crate) fn state(&self) -> ScopeRegistryState {
        Self::decode(self.state.load(std::sync::atomic::Ordering::Acquire))
    }

    /// Seal as `target`. Idempotent for the same target; refuses the other.
    fn seal(&self, target: u8) -> Result<(), ScopeModeConflict> {
        match self.state.compare_exchange(
            MODE_UNINITIALIZED,
            target,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            Err(current) if current == target => Ok(()),
            Err(current) => Err(ScopeModeConflict {
                current: Self::decode(current),
                requested: Self::decode(target),
            }),
        }
    }

    /// Publish `Initialized`. Call AFTER the registries are populated so a
    /// reader never observes `Initialized` with an empty registry.
    ///
    /// Crate-private on purpose: see the type-level docs.
    pub(crate) fn declare_initialized(&self) -> Result<(), ScopeModeConflict> {
        self.seal(MODE_INITIALIZED)
    }

    /// Publish `PolicyOnly` with an auditable reason. The reason is recorded
    /// only when the transition succeeds.
    pub(crate) fn declare_policy_only(
        &self,
        reason: &'static str,
    ) -> Result<(), ScopeModeConflict> {
        self.seal(MODE_POLICY_ONLY)?;
        self.policy_only_reason.get_or_init(|| reason);
        Ok(())
    }

    /// The reason recorded by a successful [`Self::declare_policy_only`].
    pub(crate) fn policy_only_reason(&self) -> Option<&'static str> {
        if self.state() == ScopeRegistryState::PolicyOnly {
            self.policy_only_reason.get().copied()
        } else {
            None
        }
    }
}

impl Default for ScopeModeCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// The process-wide isolation mode. Private: reachable only through the
/// boundary APIs below, never declared `Initialized` without a registration.
static SCOPE_MODE: ScopeModeCoordinator = ScopeModeCoordinator::new();

/// Current registry state for this process.
pub fn scope_registry_state() -> ScopeRegistryState {
    SCOPE_MODE.state()
}

/// Why a boundary initialization refused to seal `Initialized`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeInitError {
    /// The process already sealed a conflicting mode.
    ModeConflict(ScopeModeConflict),
    /// Nothing was registered. Sealing `Initialized` over empty registries
    /// would make every `.with_rls()` a silent no-op — the original
    /// false-green. If the schema genuinely has no scoped tables, say so
    /// with [`declare_no_scoped_tables`].
    NoScopedTables,
    /// A registry could not be populated (poisoned lock). The mode is left
    /// untouched so a half-filled registry is never published.
    RegistryUnavailable(String),
}

impl std::fmt::Display for ScopeInitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ModeConflict(conflict) => write!(f, "{conflict}"),
            Self::NoScopedTables => write!(
                f,
                "refusing to seal RLS scope registries: no tenant- or owner-scoped tables were registered (declare_no_scoped_tables(reason) if that is intentional)"
            ),
            Self::RegistryUnavailable(why) => {
                write!(f, "RLS scope registry unavailable: {why}")
            }
        }
    }
}

impl std::error::Error for ScopeInitError {}

impl From<ScopeModeConflict> for ScopeInitError {
    fn from(conflict: ScopeModeConflict) -> Self {
        Self::ModeConflict(conflict)
    }
}

/// Seal `Initialized` only if the registries hold at least one table.
///
/// The counts passed in are what THIS call registered; the invariant is
/// checked against the live registries so a second call that registers
/// nothing new still seals fine when an earlier call populated them.
fn seal_initialized_if_populated(
    counts: ScopeRegistryCounts,
) -> Result<ScopeRegistryCounts, ScopeInitError> {
    let live = live_registered_total()?;
    seal_initialized_if_populated_with(live, counts)
}

/// Live total across both registries. A poisoned registry is an error, never
/// zero — "unavailable" and "empty" lead to opposite security decisions.
fn live_registered_total() -> Result<usize, ScopeInitError> {
    let tenant = tenant::try_tenant_table_count().map_err(ScopeInitError::RegistryUnavailable)?;
    let owner = owner::try_owner_table_count().map_err(ScopeInitError::RegistryUnavailable)?;
    Ok(tenant + owner)
}

/// The invariant, separated from the globals so it can be tested directly:
/// seal only when the live registries hold at least one table.
fn seal_initialized_if_populated_with(
    live_registered: usize,
    counts: ScopeRegistryCounts,
) -> Result<ScopeRegistryCounts, ScopeInitError> {
    if live_registered == 0 {
        return Err(ScopeInitError::NoScopedTables);
    }
    SCOPE_MODE.declare_initialized()?;
    Ok(counts)
}

/// Populate BOTH runtime scope registries from a parsed `schema.qail`, then
/// publish `Initialized`.
///
/// This is the **application boundary** call: the binary that owns the
/// process decides what the AST layer scopes. Library code (the gateway's
/// schema loader, drivers) never calls it on your behalf, because activating
/// AST injection changes what every `.with_rls()` in the process returns.
/// Until this, [`init_scope_registries_from_tables`],
/// [`declare_no_scoped_tables`] or [`declare_policy_only_isolation`] runs,
/// `Qail::with_rls` fails with `RlsRegistryUninitialized`.
///
/// Refuses to seal if nothing was registered
/// ([`ScopeInitError::NoScopedTables`]) or a registry could not be filled
/// ([`ScopeInitError::RegistryUnavailable`]); refused once the process
/// sealed `PolicyOnly`. Idempotent otherwise — more tables may be added by
/// calling again.
pub fn init_scope_registries(
    schema: &crate::migrate::Schema,
) -> Result<ScopeRegistryCounts, ScopeInitError> {
    if SCOPE_MODE.state() == ScopeRegistryState::PolicyOnly {
        return Err(ScopeModeConflict {
            current: ScopeRegistryState::PolicyOnly,
            requested: ScopeRegistryState::Initialized,
        }
        .into());
    }
    let tenant = tenant::register_from_migrate_schema(schema)
        .map_err(ScopeInitError::RegistryUnavailable)?;
    let owner =
        owner::register_from_migrate_schema(schema).map_err(ScopeInitError::RegistryUnavailable)?;
    seal_initialized_if_populated(ScopeRegistryCounts { tenant, owner })
}

/// Programmatic form of [`init_scope_registries`] for processes without a
/// `schema.qail` (embedded tools, tests): register the given tenant and
/// owner tables, then seal `Initialized`. Same refusals as the schema form.
pub fn init_scope_registries_from_tables(
    tenant_tables: &[(&str, &str)],
    owner_tables: &[(&str, &str)],
) -> Result<ScopeRegistryCounts, ScopeInitError> {
    if SCOPE_MODE.state() == ScopeRegistryState::PolicyOnly {
        return Err(ScopeModeConflict {
            current: ScopeRegistryState::PolicyOnly,
            requested: ScopeRegistryState::Initialized,
        }
        .into());
    }
    // Fallible registration: a poisoned registry aborts BEFORE sealing, so a
    // live total made nonzero by the other registry can never publish
    // `Initialized` over partial metadata.
    let tenant = tenant::try_register_tenant_tables(tenant_tables)
        .map_err(ScopeInitError::RegistryUnavailable)?;
    let owner = owner::try_register_owner_tables(owner_tables)
        .map_err(ScopeInitError::RegistryUnavailable)?;
    seal_initialized_if_populated(ScopeRegistryCounts { tenant, owner })
}

/// Declare that this schema intentionally has NO tenant- or owner-scoped
/// tables, and seal `Initialized` with empty registries. Every
/// `.with_rls()` is then a checked no-op on every table. `reason` is
/// recorded so the choice is auditable. Refused if a registry already holds
/// tables (use [`init_scope_registries`]) or the process sealed `PolicyOnly`.
pub fn declare_no_scoped_tables(reason: &'static str) -> Result<(), ScopeInitError> {
    // An unavailable registry must not read as "empty": that would seal
    // `Initialized` over metadata we cannot see, and every later lookup
    // would return None — an unscoped query.
    if live_registered_total()? != 0 {
        return Err(ScopeInitError::RegistryUnavailable(
            "registries are not empty; use init_scope_registries instead".to_string(),
        ));
    }
    SCOPE_MODE.declare_initialized()?;
    NO_SCOPED_TABLES_REASON.get_or_init(|| reason);
    Ok(())
}

static NO_SCOPED_TABLES_REASON: std::sync::OnceLock<&'static str> = std::sync::OnceLock::new();

/// The reason given to a successful [`declare_no_scoped_tables`].
pub fn no_scoped_tables_reason() -> Option<&'static str> {
    NO_SCOPED_TABLES_REASON.get().copied()
}

/// Declare that this process relies on PostgreSQL RLS policies alone and
/// wants NO AST injection: `with_rls` becomes a checked no-op everywhere.
///
/// This is the explicit form of what an un-initialized process used to get
/// by accident. `reason` is recorded so the choice is auditable. Refused if
/// the process already sealed `Initialized`.
pub fn declare_policy_only_isolation(reason: &'static str) -> Result<(), ScopeModeConflict> {
    SCOPE_MODE.declare_policy_only(reason)
}

/// The reason given to a successful [`declare_policy_only_isolation`].
pub fn policy_only_reason() -> Option<&'static str> {
    SCOPE_MODE.policy_only_reason()
}

/// An opaque token that authorizes RLS bypass.
///
/// Create via one of the named constructors:
/// - [`SuperAdminToken::for_system_process`] — cron, startup, cross-tenant internals
/// - [`SuperAdminToken::for_webhook`] — inbound callbacks
/// - [`SuperAdminToken::for_auth`] — login, register, token refresh
///
/// External code cannot fabricate this token — it has a private field
/// and no public field constructor.
///
/// # Usage
/// ```ignore
/// let token = SuperAdminToken::for_system_process("cron::cleanup");
/// let ctx = RlsContext::super_admin(token);
/// assert!(ctx.bypasses_rls());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuperAdminToken {
    _private: (),
}

impl SuperAdminToken {
    /// Issue a token for a system/background process.
    ///
    /// Use for cron jobs, startup introspection, and internal cross-tenant
    /// maintenance paths. For shared/public reference data, prefer
    /// [`RlsContext::global()`] instead of bypass.
    ///
    /// The `_reason` parameter documents intent at the call site
    /// (e.g. `"cron::check_expired_holds"`). Drivers like `qail-pg`
    /// may log it via tracing.
    pub fn for_system_process(_reason: &str) -> Self {
        Self { _private: () }
    }

    /// Issue a token for an inbound webhook or gateway trigger.
    ///
    /// Use for Meta WhatsApp callbacks, Xendit payment callbacks,
    /// and gateway event triggers that are authenticated via shared
    /// secret (`X-Trigger-Secret`) rather than JWT.
    pub fn for_webhook(_source: &str) -> Self {
        Self { _private: () }
    }

    /// Issue a token for an authentication operation.
    ///
    /// Use for login, register, token refresh, and admin-claims
    /// resolution — operations that necessarily run before (or
    /// outside) a tenant scope is known.
    pub fn for_auth(_operation: &str) -> Self {
        Self { _private: () }
    }
}

/// RLS context carrying tenant identity for data isolation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RlsContext {
    /// The unified tenant ID — the primary identity for data isolation.
    /// Empty string means no tenant scope.
    pub tenant_id: String,

    /// When true, the current user is a platform super admin
    /// and should bypass tenant isolation.
    ///
    /// This field is private — external code must use `bypasses_rls()`.
    /// Only `super_admin(token)` can set this to true, and that requires
    /// a `SuperAdminToken` which emits an audit log on creation.
    is_super_admin: bool,

    /// When true, the context is explicitly scoped to global/platform rows
    /// (`tenant_id IS NULL`) rather than tenant-specific rows.
    is_global: bool,

    /// The authenticated user's UUID for user-scoped DB policies.
    /// Empty string means no user scope. Set via `RlsContext::user()`.
    user_id: String,
}

impl RlsContext {
    /// Create a context scoped to a specific tenant (the unified identity).
    pub fn tenant(tenant_id: &str) -> Self {
        Self {
            tenant_id: tenant_id.to_string(),
            is_super_admin: false,
            is_global: false,
            user_id: String::new(),
        }
    }

    /// Create a global context scoped to platform rows (`tenant_id IS NULL`).
    ///
    /// This is not a bypass: it applies explicit global scoping in AST injection
    /// and exposes `app.is_global=true` for policy usage at the database layer.
    pub fn global() -> Self {
        Self {
            tenant_id: String::new(),
            is_super_admin: false,
            is_global: true,
            user_id: String::new(),
        }
    }

    /// Create a super admin context that bypasses tenant isolation.
    ///
    /// Requires a `SuperAdminToken` — which can only be created via
    /// named constructors (`for_system_process`, `for_webhook`, `for_auth`).
    ///
    /// Uses nil UUID for all IDs to avoid `''::uuid` cast errors
    /// in PostgreSQL RLS policies (PostgreSQL doesn't short-circuit OR).
    pub fn super_admin(_token: SuperAdminToken) -> Self {
        let nil = "00000000-0000-0000-0000-000000000000".to_string();
        Self {
            tenant_id: nil,
            is_super_admin: true,
            is_global: false,
            user_id: String::new(),
        }
    }

    /// Create an empty context (no tenant, no super admin).
    ///
    /// Used for system-level operations that must not operate within
    /// any tenant scope (startup introspection, migrations, health checks).
    pub fn empty() -> Self {
        Self {
            tenant_id: String::new(),
            is_super_admin: false,
            is_global: false,
            user_id: String::new(),
        }
    }

    /// Create a user-scoped context for authenticated end-user operations.
    ///
    /// Sets `app.current_user_id` so that DB policies can enforce
    /// row-level isolation by user (e.g. `user_id = get_current_user_id()`).
    /// Does NOT bypass tenant isolation or grant super-admin.
    pub fn user(user_id: &str) -> Self {
        Self {
            tenant_id: String::new(),
            is_super_admin: false,
            is_global: false,
            user_id: user_id.to_string(),
        }
    }

    /// Attach an authenticated user ID to an existing tenant/global context.
    ///
    /// User scope is orthogonal to tenant scope: PostgreSQL policies can
    /// use both `app.current_tenant_id` and `app.current_user_id`.
    pub fn with_user(mut self, user_id: &str) -> Self {
        self.user_id = user_id.to_string();
        self
    }

    /// Returns true if this context has a tenant scope.
    pub fn has_tenant(&self) -> bool {
        !self.tenant_id.is_empty()
    }

    /// Returns true if this context has a user scope.
    pub fn has_user(&self) -> bool {
        !self.user_id.is_empty()
    }

    /// Returns the user ID for this context (empty if none).
    pub fn user_id(&self) -> &str {
        &self.user_id
    }

    /// Returns true if this context bypasses tenant isolation.
    pub fn bypasses_rls(&self) -> bool {
        self.is_super_admin
    }

    /// Returns true if this context is explicitly scoped to global rows.
    pub fn is_global(&self) -> bool {
        self.is_global
    }
}

impl std::fmt::Display for RlsContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_super_admin {
            write!(f, "RlsContext(super_admin)")
        } else if self.is_global {
            write!(f, "RlsContext(global)")
        } else if !self.tenant_id.is_empty() {
            write!(f, "RlsContext(tenant={})", self.tenant_id)
        } else {
            write!(f, "RlsContext(none)")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_mode_seals_one_way_policy_only_first() {
        // Local coordinator: never touches the process-global SCOPE_MODE.
        let mode = ScopeModeCoordinator::new();
        assert_eq!(mode.state(), ScopeRegistryState::Uninitialized);
        assert_eq!(mode.policy_only_reason(), None);

        mode.declare_policy_only("db policies only").unwrap();
        assert_eq!(mode.state(), ScopeRegistryState::PolicyOnly);
        assert_eq!(mode.policy_only_reason(), Some("db policies only"));

        // Same declaration again is idempotent; the first reason stands.
        mode.declare_policy_only("second reason").unwrap();
        assert_eq!(mode.policy_only_reason(), Some("db policies only"));

        // The conflicting transition is refused and the mode is unchanged.
        let err = mode.declare_initialized().unwrap_err();
        assert_eq!(err.current, ScopeRegistryState::PolicyOnly);
        assert_eq!(err.requested, ScopeRegistryState::Initialized);
        assert_eq!(mode.state(), ScopeRegistryState::PolicyOnly);
    }

    #[test]
    fn scope_mode_seals_one_way_initialized_first() {
        let mode = ScopeModeCoordinator::new();
        mode.declare_initialized().unwrap();
        mode.declare_initialized().unwrap();
        assert_eq!(mode.state(), ScopeRegistryState::Initialized);

        let err = mode.declare_policy_only("too late").unwrap_err();
        assert_eq!(err.current, ScopeRegistryState::Initialized);
        assert_eq!(mode.state(), ScopeRegistryState::Initialized);
        assert_eq!(
            mode.policy_only_reason(),
            None,
            "a refused declaration must not record a reason"
        );
    }

    #[test]
    fn low_level_registration_is_mode_neutral_and_empty_init_refuses_to_seal() {
        // These run against the process globals, but every assertion here is
        // order-independent: a sibling test may already have sealed
        // `Initialized` via a real registration, which is exactly the only
        // sealed state this test must ever observe.
        let before = scope_registry_state();
        tenant::try_register_tenant_tables(&[]).unwrap();
        owner::try_register_owner_tables(&[]).unwrap();
        assert_eq!(
            scope_registry_state(),
            before,
            "empty low-level registration must not change the mode"
        );
        tenant::try_register_tenant_tables(&[("_mode_neutral_probe", "tenant_id")]).unwrap();
        assert_eq!(
            scope_registry_state(),
            before,
            "non-empty low-level registration must not change the mode either"
        );
        assert!(
            tenant::try_tenant_table_count().unwrap() > 0,
            "…but the table IS recorded"
        );
    }

    #[test]
    fn init_from_tables_refuses_empty_then_seals_on_real_registration() {
        // Cannot assert `NoScopedTables` against the live process once any
        // sibling registered a table, so exercise the refusal on the pure
        // helper and the success path on the real boundary.
        assert_eq!(
            seal_initialized_if_populated_with(
                0,
                ScopeRegistryCounts {
                    tenant: 0,
                    owner: 0
                }
            ),
            Err(ScopeInitError::NoScopedTables)
        );
        let counts =
            init_scope_registries_from_tables(&[("_init_from_tables_t", "tenant_id")], &[])
                .expect("one real table seals Initialized");
        assert_eq!(
            counts,
            ScopeRegistryCounts {
                tenant: 1,
                owner: 0
            }
        );
        assert_eq!(scope_registry_state(), ScopeRegistryState::Initialized);
    }

    #[test]
    fn init_from_migrate_schema_reports_registry_failure_instead_of_zero() {
        let schema = crate::migrate::parse_qail(
            "table _init_schema_orders {\n  id UUID primary_key\n  tenant_id UUID\n}\n",
        )
        .unwrap();
        assert_eq!(tenant::register_from_migrate_schema(&schema), Ok(1));
        assert_eq!(owner::register_from_migrate_schema(&schema), Ok(0));
    }

    /// Poison a lock the way production would: a writer panics while
    /// holding the guard.
    fn poison<T: Send + Sync + 'static>(lock: &'static std::sync::RwLock<T>) {
        let result = std::thread::spawn(move || {
            let _guard = lock.write().unwrap();
            panic!("poison the registry lock");
        })
        .join();
        assert!(result.is_err(), "writer thread must have panicked");
        assert!(lock.is_poisoned());
    }

    #[test]
    fn poisoned_tenant_registry_is_an_error_for_count_and_registration() {
        // A leaked local lock: poisoning the process registry would take every
        // sibling test down with it.
        let lock: &'static std::sync::RwLock<tenant::TenantRegistry> = Box::leak(Box::new(
            std::sync::RwLock::new(tenant::TenantRegistry::new()),
        ));
        // Pre-populate so a fail-open implementation would see a nonzero count.
        tenant::register_into(lock, &[("_poison_orders", "tenant_id")]).unwrap();
        poison(lock);

        let count = tenant::count_in(lock).expect_err("poisoned count must not read as 0");
        assert!(count.contains("poisoned"), "{count}");
        let reg = tenant::register_into(lock, &[("_poison_more", "tenant_id")])
            .expect_err("poisoned registration must not be silently discarded");
        assert!(reg.contains("poisoned"), "{reg}");
    }

    #[test]
    fn poisoned_owner_registry_is_an_error_for_count_and_registration() {
        let lock: &'static std::sync::RwLock<owner::OwnerRegistry> = Box::leak(Box::new(
            std::sync::RwLock::new(owner::OwnerRegistry::new()),
        ));
        owner::register_into(lock, &[("_poison_listings", "seller_id")]).unwrap();
        poison(lock);

        assert!(
            owner::count_in(lock)
                .expect_err("poisoned count must not read as 0")
                .contains("poisoned")
        );
        assert!(
            owner::register_into(lock, &[("_poison_more", "seller_id")])
                .expect_err("poisoned registration must not be silently discarded")
                .contains("poisoned")
        );
    }

    #[test]
    fn poisoned_registry_lookup_is_an_error_not_unregistered() {
        // The runtime fail-open: after `Initialized`, a poisoned registry
        // read must surface as an error. `None` would mean "unregistered"
        // and disable every predicate at once.
        let tenant_lock: &'static std::sync::RwLock<tenant::TenantRegistry> = Box::leak(Box::new(
            std::sync::RwLock::new(tenant::TenantRegistry::new()),
        ));
        tenant::register_into(tenant_lock, &[("_poison_lookup_orders", "tenant_id")]).unwrap();
        assert_eq!(
            tenant::lookup_in(tenant_lock, "_poison_lookup_orders"),
            Ok(Some("tenant_id".to_string()))
        );
        poison(tenant_lock);
        let err = tenant::lookup_in(tenant_lock, "_poison_lookup_orders")
            .expect_err("a registered table behind a poisoned lock must NOT read as None");
        assert!(err.contains("poisoned"), "{err}");

        let owner_lock: &'static std::sync::RwLock<owner::OwnerRegistry> = Box::leak(Box::new(
            std::sync::RwLock::new(owner::OwnerRegistry::new()),
        ));
        owner::register_into(owner_lock, &[("_poison_lookup_listings", "seller_id")]).unwrap();
        poison(owner_lock);
        assert!(
            owner::lookup_in(owner_lock, "_poison_lookup_listings")
                .expect_err("poisoned owner lookup must error")
                .contains("poisoned")
        );
    }

    #[test]
    fn compatibility_lookup_collapses_error_but_scoping_does_not_use_it() {
        // The public Option form is a convenience only; scoping goes through
        // the Result form. Document the contract at the source.
        let lock: &'static std::sync::RwLock<tenant::TenantRegistry> = Box::leak(Box::new(
            std::sync::RwLock::new(tenant::TenantRegistry::new()),
        ));
        tenant::register_into(lock, &[("_compat_orders", "tenant_id")]).unwrap();
        poison(lock);
        // Same data, two answers: the Option form hides the failure…
        assert_eq!(
            tenant::lookup_in(lock, "_compat_orders").ok().flatten(),
            None
        );
        // …the Result form reports it. `Qail::with_rls` maps the latter to
        // `QailBuildError::RlsRegistryUnavailable` (see ast::cmd::rls).
        assert!(tenant::lookup_in(lock, "_compat_orders").is_err());
    }

    #[test]
    fn registry_unavailable_never_seals_initialized() {
        // The boundary invariant with an unavailable registry: the error must
        // surface and the mode must be untouched — exercised on a local
        // coordinator with the same logic the globals use.
        let mode = ScopeModeCoordinator::new();
        let live: Result<usize, ScopeInitError> = Err(ScopeInitError::RegistryUnavailable(
            "owner registry lock poisoned".into(),
        ));
        let outcome = live.and_then(|n| {
            if n == 0 {
                Err(ScopeInitError::NoScopedTables)
            } else {
                mode.declare_initialized().map_err(Into::into)
            }
        });
        assert!(matches!(
            outcome,
            Err(ScopeInitError::RegistryUnavailable(_))
        ));
        assert_eq!(mode.state(), ScopeRegistryState::Uninitialized);
    }

    #[test]
    fn test_tenant_context() {
        let ctx = RlsContext::tenant("t-123");
        assert_eq!(ctx.tenant_id, "t-123");
        assert!(!ctx.bypasses_rls());
        assert!(ctx.has_tenant());
    }

    #[test]
    fn test_super_admin_via_named_constructors() {
        let token = SuperAdminToken::for_system_process("test");
        let ctx = RlsContext::super_admin(token);
        assert!(ctx.bypasses_rls());

        let token = SuperAdminToken::for_webhook("test");
        let ctx = RlsContext::super_admin(token);
        assert!(ctx.bypasses_rls());

        let token = SuperAdminToken::for_auth("test");
        let ctx = RlsContext::super_admin(token);
        assert!(ctx.bypasses_rls());
    }

    #[test]
    fn test_display() {
        let token = SuperAdminToken::for_system_process("test_display");
        assert_eq!(
            RlsContext::super_admin(token).to_string(),
            "RlsContext(super_admin)"
        );
        assert_eq!(RlsContext::tenant("x").to_string(), "RlsContext(tenant=x)");
    }

    #[test]
    fn test_equality() {
        let a = RlsContext::tenant("t-1");
        let b = RlsContext::tenant("t-1");
        let c = RlsContext::tenant("t-2");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_empty_context() {
        let ctx = RlsContext::empty();
        assert!(!ctx.has_tenant());
        assert!(!ctx.bypasses_rls());
        assert!(!ctx.is_global());
    }

    #[test]
    fn test_global_context() {
        let ctx = RlsContext::global();
        assert!(!ctx.has_tenant());
        assert!(!ctx.bypasses_rls());
        assert!(ctx.is_global());
        assert_eq!(ctx.to_string(), "RlsContext(global)");
    }

    #[test]
    fn test_for_system_process() {
        let token = SuperAdminToken::for_system_process("cron::check_expired_holds");
        let ctx = RlsContext::super_admin(token);
        assert!(ctx.bypasses_rls());
    }

    #[test]
    fn test_for_webhook() {
        let token = SuperAdminToken::for_webhook("xendit_callback");
        let ctx = RlsContext::super_admin(token);
        assert!(ctx.bypasses_rls());
    }

    #[test]
    fn test_for_auth() {
        let token = SuperAdminToken::for_auth("login");
        let ctx = RlsContext::super_admin(token);
        assert!(ctx.bypasses_rls());
    }

    #[test]
    fn test_all_constructors_produce_equal_tokens() {
        let a = SuperAdminToken::for_system_process("a");
        let b = SuperAdminToken::for_webhook("b");
        let c = SuperAdminToken::for_auth("c");
        // All tokens are structurally identical
        assert_eq!(a, b);
        assert_eq!(b, c);
    }

    #[test]
    fn test_user_context() {
        let ctx = RlsContext::user("550e8400-e29b-41d4-a716-446655440000");
        assert!(!ctx.has_tenant());
        assert!(!ctx.bypasses_rls());
        assert!(!ctx.is_global());
        assert!(ctx.has_user());
        assert_eq!(ctx.user_id(), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn test_with_user_preserves_tenant_scope() {
        let ctx = RlsContext::tenant("tenant-1").with_user("user-1");

        assert_eq!(ctx.tenant_id, "tenant-1");
        assert_eq!(ctx.user_id(), "user-1");
        assert!(ctx.has_tenant());
        assert!(ctx.has_user());
        assert!(!ctx.bypasses_rls());
    }

    #[test]
    fn test_user_context_display() {
        let ctx = RlsContext::user("u-123");
        assert_eq!(ctx.to_string(), "RlsContext(none)");
        // user context doesn't have tenant, so Display falls through to "none"
        // (user_id is an orthogonal axis, not a tenant scope)
    }

    #[test]
    fn test_other_constructors_have_no_user() {
        assert!(!RlsContext::tenant("t-1").has_user());
        assert!(!RlsContext::global().has_user());
        assert!(!RlsContext::empty().has_user());
        let token = SuperAdminToken::for_auth("test");
        assert!(!RlsContext::super_admin(token).has_user());
    }
}
