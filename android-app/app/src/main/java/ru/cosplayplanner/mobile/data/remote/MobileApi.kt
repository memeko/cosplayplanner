package ru.cosplayplanner.mobile.data.remote

import retrofit2.http.Body
import retrofit2.http.GET
import retrofit2.http.POST
import retrofit2.http.Query
import ru.cosplayplanner.mobile.data.model.MobileBootstrapResponse
import ru.cosplayplanner.mobile.data.model.MobileSyncRequest
import ru.cosplayplanner.mobile.data.model.MobileSyncResponse

interface MobileApi {
    @GET("/api/mobile/bootstrap")
    suspend fun bootstrap(@Query("since") since: String? = null): MobileBootstrapResponse

    @POST("/api/mobile/sync")
    suspend fun sync(@Body request: MobileSyncRequest): MobileSyncResponse
}
