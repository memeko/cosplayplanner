package ru.cosplayplanner.mobile.data.remote

import retrofit2.http.Body
import retrofit2.Response
import retrofit2.http.Field
import retrofit2.http.FormUrlEncoded
import retrofit2.http.GET
import retrofit2.http.POST
import retrofit2.http.Query
import ru.cosplayplanner.mobile.data.model.MobileBootstrapResponse
import ru.cosplayplanner.mobile.data.model.MobileSyncRequest
import ru.cosplayplanner.mobile.data.model.MobileSyncResponse
import ru.cosplayplanner.mobile.data.model.*
import okhttp3.ResponseBody

interface MobileApi {
    @POST("/api/mobile/login")
    suspend fun mobileLogin(@Body request: MobileLoginRequest): Response<MobileLoginResponse>

    @FormUrlEncoded
    @POST("/login")
    suspend fun webLogin(
        @Field("login") login: String,
        @Field("password") password: String,
    ): Response<ResponseBody>

    @POST("/api/mobile/logout")
    suspend fun logout(): Map<String, Any?>
    @GET("/api/mobile/bootstrap")
    suspend fun bootstrap(@Query("since") since: String? = null): MobileBootstrapResponse

    @POST("/api/mobile/sync")
    suspend fun sync(@Body request: MobileSyncRequest): MobileSyncResponse

    @POST("/api/mobile/in-progress/sync")
    suspend fun syncInProgress(@Body request: MobileInProgressSyncRequest): MobileInProgressSyncResponse

    @GET("/api/mobile/pigeons")
    suspend fun pigeons(): PigeonInboxResponse

    @POST("/api/mobile/pigeons")
    suspend fun sendPigeon(@Body request: PigeonSendRequest): PigeonSendResponse
}
