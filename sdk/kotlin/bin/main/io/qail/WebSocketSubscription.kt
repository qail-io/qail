package io.qail

import kotlinx.coroutines.Job

/**
 * Concrete WebSocket subscription implementation.
 *
 * Wraps a coroutine [Job] that runs the WebSocket receive loop.
 * Calling [unsubscribe] cancels the coroutine, closing the connection.
 */
internal class WebSocketSubscriptionImpl(
    private val channel: String,
    private val onMessage: (String) -> Unit,
) : QailSubscription {

    internal var job: Job? = null
    private var _active = true

    override val active: Boolean
        get() = _active && (job?.isActive == true)

    override fun unsubscribe() {
        _active = false
        job?.cancel()
    }
}
