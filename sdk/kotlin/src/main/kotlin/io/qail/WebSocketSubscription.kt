package io.qail

import kotlinx.coroutines.Job

/**
 * Concrete WebSocket subscription implementation.
 *
 * Wraps a coroutine [Job] that runs the WebSocket receive loop.
 * Calling [unsubscribe] cancels the coroutine, closing the connection.
 */
internal class WebSocketSubscriptionImpl(
) : QailSubscription {

    internal var job: Job? = null
    @Volatile
    private var subscribed = true

    override val active: Boolean
        get() = subscribed && (job?.isActive == true)

    override fun unsubscribe() {
        subscribed = false
        job?.cancel()
    }

    internal fun markClosed() {
        subscribed = false
    }
}
