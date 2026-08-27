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
        assertEquals(1.toByte(), dispatcher.tryAdjustParallelism(1), "a single unit of headroom should be granted in full")
        val extraStarted = CountDownLatch(1)
        dispatcher.dispatch(EmptyCoroutineContext, Runnable { extraStarted.countDown() })
        assertTrue(
            extraStarted.await(10, TimeUnit.SECONDS),
            "adjustParallelism(1) should let one extra task run concurrently with the corePoolSize busy ones"
        )

        assertEquals((-1).toByte(), dispatcher.tryAdjustParallelism(-1), "the previously granted unit of headroom should be reclaimed in full")
        release.countDown()
    }

    @Test
    fun testDefaultLikeDispatcherAdjustParallelismByMultipleUnitsGrantsExactlyThatMany() {
        corePoolSize = 2
        maxPoolSize = 64
        // tryAdjustParallelism(delta) increments/decrements one unit at a time internally; make sure it
        // does so exactly `delta` times rather than off by one in either direction.
        assertEquals(3.toByte(), dispatcher.tryAdjustParallelism(3), "adjustParallelism(3) should grant exactly 3 extra units of parallelism")
        assertEquals((-3).toByte(), dispatcher.tryAdjustParallelism(-3), "adjustParallelism(-3) should reclaim exactly the 3 units granted above")
    }

    @Test
    fun testDefaultLikeDispatcherAdjustParallelismByMinusOneIsNoOpWithoutPriorIncrease() {
        corePoolSize = 2
        // No matching adjustParallelism(1) beforehand -> zero legitimate headroom, so this must not
        // shrink the pool below corePoolSize.
        assertEquals(0.toByte(), dispatcher.tryAdjustParallelism(-1), "there is no outstanding compensation to reclaim, so nothing should be adjusted")

        val started = CountDownLatch(corePoolSize)
        val release = CountDownLatch(1)
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
        release.countDown()
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

        assertEquals(1.toByte(), soft.tryAdjustParallelism(1), "a single unit of headroom should be granted in full")
        val extraStarted = CountDownLatch(1)
        soft.dispatch(EmptyCoroutineContext, Runnable { extraStarted.countDown() })
        assertTrue(
            extraStarted.await(10, TimeUnit.SECONDS),
            "adjustParallelism(1) should let one extra task run concurrently on the soft-limited view"
        )

        assertEquals((-1).toByte(), soft.tryAdjustParallelism(-1), "the previously granted unit of headroom should be reclaimed in full")
        release.countDown()
    }

    @Test
    fun testIoLikeDispatcherAdjustParallelismByMinusOneCannotShrinkBelowInitialParallelism() {
        val parallelism = 2
        val soft = softBlockingDispatcher(parallelism)

        // A soft-limited view must never drop below its initial parallelism on its own: there is no
        // outstanding compensation to reclaim, so this must be a no-op.
        assertEquals(0.toByte(), soft.tryAdjustParallelism(-1), "a soft-limited view must not shrink below its initial parallelism")

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

    /**
     * Regression test for a race in [SoftLimitedDispatcher.tryAdjustParallelism]: the bounds check
     * ("would this delta push totalParallelism below the allowed minimum?") and the update used to be
     * two separate steps that were not atomic with respect to each other. Two concurrent callers could
     * both read the same pre-update `totalParallelism`, both conclude the reclaim is within bounds, and
     * both apply their delta - overshooting the limit even though each individual check looked valid.
     *
     * Here exactly one unit of headroom is granted, then two threads race to reclaim it with `-1`.
     * Without a lock serializing check-and-update, both `-1` calls can occasionally succeed, which
     * would shrink the dispatcher below its initial parallelism - something no single caller is allowed
     * to do without a matching grant.
     */
    @Test
    fun testIoLikeDispatcherConcurrentAdjustParallelismNeverOverdraftsHeadroom() {
        val parallelism = 2
        val soft = softBlockingDispatcher(parallelism)
        val executor = Executors.newFixedThreadPool(2)
        try {
            repeat(10_000 * stressTestMultiplierSqrt) {
                assertEquals(1.toByte(), soft.tryAdjustParallelism(1), "should always be able to grant a single unit of headroom")

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
