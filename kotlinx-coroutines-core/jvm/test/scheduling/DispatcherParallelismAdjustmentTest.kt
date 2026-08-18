package kotlinx.coroutines.scheduling

import kotlinx.coroutines.internal.*
import org.junit.Test
import java.util.concurrent.*
import kotlin.coroutines.*
import kotlin.test.*

/**
 * Exercises `CoroutineDispatcher.adjustParallelism(+1)` / `(-1)` at the dispatcher level (as opposed to
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
        dispatcher.adjustParallelism(1)
        val extraStarted = CountDownLatch(1)
        dispatcher.dispatch(EmptyCoroutineContext, Runnable { extraStarted.countDown() })
        assertTrue(
            extraStarted.await(10, TimeUnit.SECONDS),
            "adjustParallelism(1) should let one extra task run concurrently with the corePoolSize busy ones"
        )

        dispatcher.adjustParallelism(-1)
        release.countDown()
    }

    @Test
    fun testDefaultLikeDispatcherAdjustParallelismByMinusOneIsNoOpWithoutPriorIncrease() {
        corePoolSize = 2
        // No matching adjustParallelism(1) beforehand -> zero legitimate headroom, so this must not
        // shrink the pool below corePoolSize.
        dispatcher.adjustParallelism(-1)

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

        soft.adjustParallelism(1)
        val extraStarted = CountDownLatch(1)
        soft.dispatch(EmptyCoroutineContext, Runnable { extraStarted.countDown() })
        assertTrue(
            extraStarted.await(10, TimeUnit.SECONDS),
            "adjustParallelism(1) should let one extra task run concurrently on the soft-limited view"
        )

        soft.adjustParallelism(-1)
        release.countDown()
    }

    @Test
    fun testIoLikeDispatcherAdjustParallelismByMinusOneShrinksBelowInitialParallelism() {
        val parallelism = 2
        val soft = softBlockingDispatcher(parallelism)

        soft.adjustParallelism(-1)

        val started = CountDownLatch(parallelism)
        val release = CountDownLatch(1)
        repeat(parallelism) {
            soft.dispatch(EmptyCoroutineContext, Runnable {
                started.countDown()
                release.await()
            })
        }
        assertFalse(
            started.await(2, TimeUnit.SECONDS),
            "adjustParallelism(-1) should have reduced concurrency below the initial parallelism"
        )
        assertEquals(1L, started.count)
        release.countDown()
    }
}
