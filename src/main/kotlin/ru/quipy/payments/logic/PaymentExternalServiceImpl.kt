package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val mapper = ObjectMapper().registerKotlinModule()

        const val MAX_RETRY_ATTEMPTS = 1000
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = LeakingBucketRateLimiter(
        rate = if (rateLimitPerSec > 3) rateLimitPerSec.toLong() - 1 else 1,
        window = Duration.ofSeconds(1),
        bucketSize = if (rateLimitPerSec > 3) rateLimitPerSec - 2 else 1
    )

    private val ongoingWindow = NonBlockingOngoingWindow(
        maxWinSize = parallelRequests,
    )
    private val semaphore = Semaphore(parallelRequests)

    private val client: HttpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .connectTimeout(Duration.ofSeconds(3))
        .build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        fun attemptRequest(attempt: Int) {
            if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }
                return
            }

            if (!semaphore.tryAcquire()) {
                logger.warn("[$accountName] Too many concurrent requests")
                return
            }

            if (!rateLimiter.tick()) {
                logger.warn("[$accountName] Sliding window rate limit")
                semaphore.release()
                return
            }

            if (ongoingWindow.putIntoWindow() is NonBlockingOngoingWindow.WindowResponse.Fail) {
                logger.warn("[$accountName] Ongoing window full")
                semaphore.release()
                return
            }

            val request = HttpRequest.newBuilder()
                .uri(URI("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
                .version(HttpClient.Version.HTTP_2)
                .POST(HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofMillis(deadline - now()))
                .build()

            client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .orTimeout(deadline - now(), TimeUnit.MILLISECONDS)
                .thenAcceptAsync { response ->
                    try {
                        val body = try {
                            mapper.readValue(response.body(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Failed to parse response for txId: $transactionId, payment: $paymentId, code: ${response.statusCode()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        logger.info("[$accountName] Response for txId: $transactionId, payment: $paymentId: ${body.result}, message: ${body.message}")
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Unexpected error processing response", e)
                    }
                }
                .exceptionally { ex ->
                    if (attempt < MAX_RETRY_ATTEMPTS) {
                        logger.warn("[$accountName] Retrying paymentId=$paymentId attempt=$attempt due to ${ex.message}")
                        attemptRequest(attempt + 1)
                    } else {
                        logger.error("[$accountName] Payment failed for paymentId=$paymentId after $attempt attempts", ex)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = ex.message)
                        }
                    }
                    null
                }
                .whenComplete { _, _ ->
                    semaphore.release()
                    ongoingWindow.releaseWindow()
                }
        }

        attemptRequest(1)
    }

    private fun isDeadlineExceeded(deadline: Long): Boolean {
        return LocalDateTime.now().isAfter(
            Instant.ofEpochMilli(deadline).atZone(ZoneId.systemDefault()).toLocalDateTime()
        )
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()