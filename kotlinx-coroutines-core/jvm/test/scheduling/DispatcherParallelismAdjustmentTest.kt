package kotlinx.coroutines.scheduling

import kotlinx.coroutines.internal.*
import kotlinx.coroutines.testing.*
import org.junit.Test
import java.util.concurrent.*
import kotlin.coroutines.*
import kotlin.test.*

/**
 * Exercises `CoroutineDispatcher.tryAdjustParallelism(+1)` / `(-1)` at the dispatcher level (as opposed to
 * [CpuParallelismControlTest], which drives [CoroutineScheduler]'s increase/decrease methods directly), for
 * both flavors of dispatcher that implement [SoftLimitedParallelism]:
 *  - a plain [SchedulerCoroutineDispatcher] (what backs `Dispatchers.Default`)
 *  - a [SoftLimitedDispatcher] view on top of it (what backs `Dispatchers.IO`)
 */
class DispatcherParallelismAdjustmentTest : SchedulerTestBase() {

    @Test
    fun testDefaultLikeDispatcherAdjustParallelismByOneLetsExtraTaskRunConcurrently() {
        corePoolSize = 2
        val started = CountDownLatch(corePoolSize)
        val release = CountDownLatch(1)
        try {
            repeat(corePoolSize) {
                dispatcher.dispatch(EmptyCoroutineContext, Runnable {
                    started.countDown()
                    release.await()
                })
            }
            assertTrue(started.await(10, TimeUnit.SECONDS))

            // All corePoolSize permits are held by the busy tasks above (which are still blocked on
            // release), so this extra task can only start if adjustParallelism(1) genuinely grants a
            // new permit.
            assertEquals(
                1,
                dispatcher.tryAdjustParallelism(1),
                "a single unit of headroom should be granted in full"
            )
            val extraStarted = CountDownLatch(1)
            dispatcher.dispatch(EmptyCoroutineContext, Runnable { extraStarted.countDown() })
            assertTrue(
                extraStarted.await(10, TimeUnit.SECONDS),
                "adjustParallelism(1) should let one extra task run concurrently with the corePoolSize busy ones"
            )

            assertEquals(
                -1,
                dispatcher.tryAdjustParallelism(-1),
                "the previously granted unit of headroom should be reclaimed in full"
            )
        } finally {
            release.countDown()
        }
    }

    @Test
    fun testDefaultLikeDispatcherAdjustParallelismByMultipleUnitsGrantsExactlyThatMany() {
        corePoolSize = 2
        maxPoolSize = 64
        // tryAdjustParallelism(delta) increments/decrements one unit at a time internally; make sure it
        // does so exactly `delta` times rather than off by one in either direction.
        assertEquals(3, dispatcher.tryAdjustParallelism(3), "adjustParallelism(3) should grant exactly 3 extra units of parallelism")
        assertEquals(-3, dispatcher.tryAdjustParallelism(-3), "adjustParallelism(-3) should reclaim exactly the 3 units granted above")
    }

    @Test
    fun testDefaultLikeDispatcherAdjustParallelismByMinusOneIsNoOpWithoutPriorIncrease() {
        corePoolSize = 2
        // No matching adjustParallelism(1) beforehand -> zero legitimate headroom, so this must not
        // shrink the pool below corePoolSize.
        assertEquals(0, dispatcher.tryAdjustParallelism(-1), "there is no outstanding compensation to reclaim, so nothing should be adjusted")

        val started = CountDownLatch(corePoolSize)
        val release = CountDownLatch(1)
        try {
            repeat(corePoolSize) {
                dispatcher.dispatch(EmptyCoroutineContext, Runnable {
                    started.countDown()
                    release.await()
                })
            }
            assertTrue(
                started.await(10, TimeUnit.SECONDS),
                "adjustParallelism(-1) without a matching increase must not shrink the pool below corePoolSize"
            )
        } finally {
            release.countDown()
        }
    }

    @Test(timeout = 5_000L)
    fun testDefaultLikeDispatcherHandlesExtremeAdjustmentsPromptly() {
        corePoolSize = 1
        maxPoolSize = 64

        val increased = dispatcher.tryAdjustParallelism(Int.MAX_VALUE)

        assertEquals(
            MAX_OUTSTANDING_CPU_COMPENSATIONS,
            increased,
            "Int.MAX_VALUE should apply adjustments until the first failure"
        )

        val decreased = dispatcher.tryAdjustParallelism(Int.MIN_VALUE)

        assertEquals(
            -increased,
            decreased,
            "Int.MIN_VALUE should reclaim all available adjustments and stop at the first failure"
        )

        assertEquals(
            0,
            dispatcher.tryAdjustParallelism(-1),
            "All outstanding adjustments should already be reclaimed"
        )
    }

    @Test
    fun testIoLikeDispatcherAdjustParallelismByOneLetsExtraTaskRunConcurrently() {
        val parallelism = 2
        val soft = softBlockingDispatcher(parallelism)

        val started = CountDownLatch(parallelism)
        val release = CountDownLatch(1)
        repeat(parallelism) {
            soft.dispatch(EmptyCoroutineContext, Runnable {
                started.countDown()
                release.await()
            })
        }
        assertTrue(started.await(10, TimeUnit.SECONDS))

        assertEquals(1, soft.tryAdjustParallelism(1), "a single unit of headroom should be granted in full")
        val extraStarted = CountDownLatch(1)
        soft.dispatch(EmptyCoroutineContext, Runnable { extraStarted.countDown() })
        assertTrue(
            extraStarted.await(10, TimeUnit.SECONDS),
            "adjustParallelism(1) should let one extra task run concurrently on the soft-limited view"
        )

        assertEquals(-1, soft.tryAdjustParallelism(-1), "the previously granted unit of headroom should be reclaimed in full")
        release.countDown()
    }

    @Test
    fun testIoLikeDispatcherAdjustParallelismByMinusOneCannotShrinkBelowInitialParallelism() {
        val parallelism = 2
        val soft = softBlockingDispatcher(parallelism)

        // A soft-limited view must never drop below its initial parallelism on its own: there is no
        // outstanding compensation to reclaim, so this must be a no-op.
        assertEquals(0, soft.tryAdjustParallelism(-1), "a soft-limited view must not shrink below its initial parallelism")

        val started = CountDownLatch(parallelism)
        val release = CountDownLatch(1)
        repeat(parallelism) {
            soft.dispatch(EmptyCoroutineContext, Runnable {
                started.countDown()
                release.await()
            })
        }
        assertTrue(
            started.await(10, TimeUnit.SECONDS),
            "adjustParallelism(-1) without prior headroom must not reduce concurrency below the initial parallelism"
        )
        release.countDown()
    }

    @Test
    fun testIoLikeDispatcherAdjustParallelismByOneStartsAlreadyQueuedWork() {
        val parallelism = 1
        val soft = softBlockingDispatcher(parallelism)

        val started = CountDownLatch(parallelism)
        val release = CountDownLatch(1)
        try {
            repeat(parallelism) {
                soft.dispatch(EmptyCoroutineContext, Runnable {
                    started.countDown()
                    release.await()
                })
            }
            assertTrue(started.await(10, TimeUnit.SECONDS))

            // No permit is free for this task, so it sits in the internal queue instead of running.
            val queuedStarted = CountDownLatch(1)
            soft.dispatch(EmptyCoroutineContext, Runnable { queuedStarted.countDown() })
            assertFalse(
                queuedStarted.await(500, TimeUnit.MILLISECONDS),
                "sanity check: the second task should still be queued, since no permit was available for it"
            )

            assertEquals(1, soft.tryAdjustParallelism(1), "a single unit of headroom should be granted in full")
            // The sole worker above is still stuck on release.await() (simulating a deadlocked task), so the
            // only way the already-queued task can start now is if adjustParallelism(1) itself kicks the queue.
            assertTrue(
                queuedStarted.await(1000, TimeUnit.MILLISECONDS),
                "adjustParallelism(1) must start already-queued work -- otherwise it cannot unblock work stuck " +
                    "behind a task that will never return on its own, defeating deadlock recovery"
            )
        } finally {
            release.countDown()
        }
    }

    @Test
    fun testBatchSoftIncreaseStartsEveryRequiredQueuedWorker() {
        val soft = softBlockingDispatcher(parallelism = 1)

        val originalStarted = CountDownLatch(1)
        val releaseOriginal = CountDownLatch(1)

        val queuedStarted = CountDownLatch(2)
        val releaseQueued = CountDownLatch(1)

        try {
            soft.dispatch(
                EmptyCoroutineContext,
                Runnable {
                    originalStarted.countDown()
                    releaseOriginal.await()
                }
            )

            assertTrue(
                originalStarted.await(10, TimeUnit.SECONDS)
            )

            repeat(2) {
                soft.dispatch(
                    EmptyCoroutineContext,
                    Runnable {
                        queuedStarted.countDown()
                        releaseQueued.await()
                    }
                )
            }

            assertEquals(
                2,
                soft.tryAdjustParallelism(2)
            )

            assertTrue(
                queuedStarted.await(2, TimeUnit.SECONDS),
                "A +2 adjustment must start two queued workers. " +
                    "Starting only one may leave a dependency cycle deadlocked."
            )
        } finally {
            releaseOriginal.countDown()
            releaseQueued.countDown()
        }
    }

    @Test
    fun testIoLikeDispatcherConcurrentAdjustParallelismNeverOverdraftsHeadroom() {
        val parallelism = 2
        val soft = softBlockingDispatcher(parallelism)
        val executor = Executors.newFixedThreadPool(2)
        try {
            repeat(10_000 * stressTestMultiplierSqrt) {
                assertEquals(1, soft.tryAdjustParallelism(1), "should always be able to grant a single unit of headroom")

                val barrier = CyclicBarrier(2)
                val futures = List(2) {
                    executor.submit(Callable {
                        barrier.await(10, TimeUnit.SECONDS)
                        soft.tryAdjustParallelism(-1)
                    })
                }
                val results = futures.map { it.get(10, TimeUnit.SECONDS) }

                assertEquals(
                    -1, results.sumOf { it.toInt() },
                    "exactly one of the two concurrent reclaims should succeed since only one unit of headroom was granted, got $results"
                )
            }
        } finally {
            executor.shutdown()
        }
    }
}
