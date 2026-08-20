package kotlinx.coroutines.scheduling

import kotlinx.coroutines.testing.*
import org.junit.Test
import java.lang.Runnable
import java.util.concurrent.*
import kotlin.test.*

class CpuParallelismControlTest : TestBase() {

    private fun awaitCondition(timeoutMs: Long = 5_000, intervalMs: Long = 10, condition: () -> Boolean) {
        val deadlineNs = System.nanoTime() + timeoutMs * 1_000_000
        while (!condition()) {
            if (System.nanoTime() >= deadlineNs) {
                assertTrue(condition(), "Condition not met within ${timeoutMs}ms")
                return
            }
            Thread.sleep(intervalMs)
        }
    }

    private fun CoroutineScheduler.liveWorkerThreads(): List<CoroutineScheduler.Worker> =
        Thread.getAllStackTraces().keys.filterIsInstance<CoroutineScheduler.Worker>().filter { it.scheduler === this }

    // CoroutineScheduler.toString() already reports these two figures for diagnostic/logging
    // purposes; parsing it here avoids adding fields to production code purely for test visibility.
    private val CREATED_WORKERS_REGEX = Regex("created workers= (\\d+)")
    private val CPUS_ACQUIRED_REGEX = Regex("CPUs acquired = (-?\\d+)")

    private fun CoroutineScheduler.createdWorkersSnapshot(): Int =
        CREATED_WORKERS_REGEX.find(toString())!!.groupValues[1].toInt()

    private fun CoroutineScheduler.availableCpuPermitsSnapshot(): Int =
        corePoolSize - CPUS_ACQUIRED_REGEX.find(toString())!!.groupValues[1].toInt()

    @Test
    fun testIncreaseCpuParallelismCreatesWorkerFromZero() {
        val corePoolSize = 1
        CoroutineScheduler(corePoolSize, corePoolSize, schedulerName = "IncreaseFromZero").use { scheduler ->
            assertEquals(0, scheduler.createdWorkersSnapshot())
            assertEquals(corePoolSize, scheduler.availableCpuPermitsSnapshot())

            assertTrue(scheduler.tryIncrementCpuParallelism(), "tryIncrementCpuParallelism() should succeed when there is room to grow")

            assertEquals(1, scheduler.createdWorkersSnapshot())
            assertEquals(corePoolSize + 1, scheduler.availableCpuPermitsSnapshot())
            awaitCondition { scheduler.liveWorkerThreads().isNotEmpty() }
        }
    }

    @Test
    fun testIncreaseCpuParallelismFailsAfterTermination() {
        val corePoolSize = 1
        val scheduler = CoroutineScheduler(corePoolSize, corePoolSize, schedulerName = "Terminated")
        scheduler.close()
        assertFalse(scheduler.tryIncrementCpuParallelism(), "tryIncrementCpuParallelism() must fail once the scheduler is terminated")
    }

    @Test
    fun testIncreaseCpuParallelismWhenAllPermitsHeld() {
        val corePoolSize = 2
        CoroutineScheduler(corePoolSize, corePoolSize, schedulerName = "IncreaseAllHeld").use { scheduler ->
            assertEquals(corePoolSize, scheduler.availableCpuPermitsSnapshot())

            val started = CountDownLatch(corePoolSize)
            val release = CountDownLatch(1)
            repeat(corePoolSize) {
                scheduler.dispatch(Runnable {
                    started.countDown()
                    release.await()
                })
            }
            assertTrue(started.await(10, TimeUnit.SECONDS))
            awaitCondition { scheduler.availableCpuPermitsSnapshot() == 0 }

            assertTrue(scheduler.tryIncrementCpuParallelism())
            awaitCondition { scheduler.availableCpuPermitsSnapshot() == 1 }

            // Prove the increase is a real, reclaimable credit (not just a transient bump): a matching
            // decrease consumes it, and once the busy workers finish, the pool settles exactly back at
            // corePoolSize rather than corePoolSize + 1.
            assertTrue(scheduler.tryDecreaseCpuParallelism())
            release.countDown()

            awaitCondition { scheduler.availableCpuPermitsSnapshot() == corePoolSize }
        }
    }

    @Test
    fun testIncreaseThenDecreaseCpuParallelismRoundTrip() {
        val corePoolSize = 2
        CoroutineScheduler(corePoolSize, corePoolSize, schedulerName = "RoundTrip").use { scheduler ->
            assertEquals(corePoolSize, scheduler.availableCpuPermitsSnapshot())

            val n = 3
            repeat(n) { assertTrue(scheduler.tryIncrementCpuParallelism()) }
            awaitCondition { scheduler.availableCpuPermitsSnapshot() == corePoolSize + n }

            repeat(n) {
                assertTrue(scheduler.tryDecreaseCpuParallelism())
                // Drive a trivial completed non-blocking task so some CPU-permit-holding worker calls
                // tryDecompensateCpu() right after running it.
                val latch = CountDownLatch(1)
                scheduler.dispatch(Runnable { latch.countDown() })
                assertTrue(latch.await(5, TimeUnit.SECONDS))
            }

            awaitCondition { scheduler.availableCpuPermitsSnapshot() == corePoolSize }
        }
    }

    @Test
    fun testDecreaseCpuParallelismCannotGoBelowCorePoolSize() {
        val corePoolSize = 2
        CoroutineScheduler(corePoolSize, corePoolSize, schedulerName = "FloorInvariant").use { scheduler ->
            // No prior increaseCpuParallelism() calls -> zero legitimate headroom, so every one of these
            // should be a no-op, as reported by its own return value.
            repeat(corePoolSize + 3) { assertFalse(scheduler.tryDecreaseCpuParallelism()) }

            // Drive corePoolSize workers to actually acquire and then relinquish their CPU permits, so that
            // any (incorrectly) accepted decompensation requests get a chance to permanently drain permits.
            val started = CountDownLatch(corePoolSize)
            val release = CountDownLatch(1)
            repeat(corePoolSize) {
                scheduler.dispatch(Runnable {
                    started.countDown()
                    release.await()
                })
            }
            assertTrue(started.await(10, TimeUnit.SECONDS))
            release.countDown()

            awaitCondition(timeoutMs = 3_000) { scheduler.availableCpuPermitsSnapshot() == corePoolSize }
            assertEquals(corePoolSize, scheduler.availableCpuPermitsSnapshot())
        }
    }

    @Test
    fun testDecreaseCpuParallelismInPlaceWhenPermitIsFree() {
        val corePoolSize = 2
        CoroutineScheduler(corePoolSize, corePoolSize, schedulerName = "DecreaseInPlace").use { scheduler ->
            assertEquals(corePoolSize, scheduler.availableCpuPermitsSnapshot())

            // Nothing is using the pool, so the permit increaseCpuParallelism() grants here is
            // never claimed by any worker -- it just sits free.
            assertTrue(scheduler.tryIncrementCpuParallelism())
            awaitCondition { scheduler.availableCpuPermitsSnapshot() == corePoolSize + 1 }

            // With a free permit available, decreaseCpuParallelism() should steal it back via
            // tryAcquireCpuPermit() synchronously, with no need for any worker to complete a task
            // or to go through cpuDecompensationRequests.
            assertTrue(scheduler.tryDecreaseCpuParallelism())

            assertEquals(corePoolSize, scheduler.availableCpuPermitsSnapshot())
        }
    }

    @Test
    fun testDecreaseCpuParallelismGoesThroughDecompensationRequestWhenNoPermitIsFree() {
        val corePoolSize = 1
        CoroutineScheduler(corePoolSize, corePoolSize + 1, schedulerName = "DecreaseViaRequest").use { scheduler ->
            val started1 = CountDownLatch(1)
            val release1 = CountDownLatch(1)
            scheduler.dispatch(Runnable {
                started1.countDown()
                release1.await()
            })
            assertTrue(started1.await(10, TimeUnit.SECONDS))
            awaitCondition { scheduler.availableCpuPermitsSnapshot() == 0 }

            // Ask the worker running task1, from this thread, to compensate for going into a
            // blocking wait -- the same cross-thread ParallelismCompensation trick production
            // watchdogs use. This marks the worker as non-CPU (opening a slot for a replacement
            // CPU worker) and grants one genuine extra permit via increaseCpuParallelism(), while
            // the worker itself keeps holding onto its own original permit throughout.
            val worker1 = scheduler.liveWorkerThreads().single()
            (worker1 as ParallelismCompensation).increaseParallelismAndLimit()

            // The freed-up slot lets a second worker spin up and claim the newly granted permit
            // for a second busy task, so both the original and the extra permit end up genuinely
            // held at the same time.
            val started2 = CountDownLatch(1)
            val release2 = CountDownLatch(1)
            scheduler.dispatch(Runnable {
                started2.countDown()
                release2.await()
            })
            assertTrue(started2.await(10, TimeUnit.SECONDS))
            awaitCondition { scheduler.availableCpuPermitsSnapshot() == 0 }

            // No permit is free right now, so decreaseCpuParallelism() can't steal one in place
            // and must fall back to registering a decompensation request instead. It still
            // succeeds because there is a genuine outstanding compensation (granted above via
            // increaseParallelismAndLimit()) for it to claim.
            assertTrue(scheduler.tryDecreaseCpuParallelism())

            release1.countDown()
            release2.countDown()
            awaitCondition { scheduler.availableCpuPermitsSnapshot() == corePoolSize }
        }
    }

    @Test
    fun testShutdownWithUnclaimedOutstandingCompensationDoesNotHang() {
        val corePoolSize = 2
        CoroutineScheduler(corePoolSize, corePoolSize, schedulerName = "ShutdownUnclaimed").use { scheduler ->
            // Leaves outstandingCpuCompensations == 1 with nobody ever having claimed the extra
            // permit -- shutdown() must drain this itself before its availableCpuPermits ==
            // corePoolSize assertion, via decreaseCpuParallelism().
            assertTrue(scheduler.tryIncrementCpuParallelism())
            awaitCondition { scheduler.availableCpuPermitsSnapshot() == corePoolSize + 1 }

            val shutdownThread = Thread { scheduler.close() }
            shutdownThread.start()
            shutdownThread.join(10_000)
            assertFalse(shutdownThread.isAlive, "shutdown() did not complete in time -- likely hung draining outstandingCpuCompensations")
        }
    }

    @Test
    fun testShutdownWithDecompensationRequestDuringDrainSettlesAtCorePoolSize() {
        val corePoolSize = 1
        CoroutineScheduler(corePoolSize, corePoolSize + 1, schedulerName = "ShutdownViaRequest").use { scheduler ->
            val started1 = CountDownLatch(1)
            val release1 = CountDownLatch(1)
            scheduler.dispatch(Runnable {
                started1.countDown()
                release1.await()
            })
            assertTrue(started1.await(10, TimeUnit.SECONDS))
            awaitCondition { scheduler.availableCpuPermitsSnapshot() == 0 }

            // Same cross-thread compensation trick as above: grants one genuine extra permit that
            // a second busy worker then actively claims, so both permits are held when shutdown()
            // starts -- forcing its internal drain loop through the decompensation-request
            // fallback rather than an in-place decrement.
            val worker1 = scheduler.liveWorkerThreads().single()
            (worker1 as ParallelismCompensation).increaseParallelismAndLimit()

            val started2 = CountDownLatch(1)
            val release2 = CountDownLatch(1)
            scheduler.dispatch(Runnable {
                started2.countDown()
                release2.await()
            })
            assertTrue(started2.await(10, TimeUnit.SECONDS))
            awaitCondition { scheduler.availableCpuPermitsSnapshot() == 0 }

            // Start shutdown while both permits are still held, then let the busy workers finish
            // so shutdown's forced worker termination can settle the registered decompensation
            // request, same as it would for a normal task completion.
            val shutdownThread = Thread { scheduler.close() }
            shutdownThread.start()
            release1.countDown()
            release2.countDown()

            shutdownThread.join(10_000)
            assertFalse(shutdownThread.isAlive, "shutdown() did not complete in time")
        }
    }
}
