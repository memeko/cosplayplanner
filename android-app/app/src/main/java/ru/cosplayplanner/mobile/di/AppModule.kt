package ru.cosplayplanner.mobile.di

import android.content.Context
import androidx.room.Room
import com.squareup.moshi.Moshi
import com.squareup.moshi.kotlin.reflect.KotlinJsonAdapterFactory
import dagger.Module
import dagger.Provides
import dagger.hilt.InstallIn
import dagger.hilt.android.qualifiers.ApplicationContext
import dagger.hilt.components.SingletonComponent
import okhttp3.OkHttpClient
import okhttp3.logging.HttpLoggingInterceptor
import retrofit2.Retrofit
import retrofit2.converter.moshi.MoshiConverterFactory
import ru.cosplayplanner.mobile.data.local.AppDatabase
import ru.cosplayplanner.mobile.data.remote.MobileApi
import javax.inject.Singleton
import ru.cosplayplanner.mobile.data.remote.PersistentCookieJar
import androidx.room.migration.Migration
import androidx.sqlite.db.SupportSQLiteDatabase

@Module
@InstallIn(SingletonComponent::class)
object AppModule {
    @Provides
    @Singleton
    fun provideMoshi(): Moshi = Moshi.Builder()
        .addLast(KotlinJsonAdapterFactory())
        .build()

    @Provides
    @Singleton
    fun provideOkHttpClient(@ApplicationContext context: Context): OkHttpClient {
        val logger = HttpLoggingInterceptor().apply { level = HttpLoggingInterceptor.Level.BASIC }
        return OkHttpClient.Builder()
            .cookieJar(PersistentCookieJar(context))
            .addInterceptor(logger)
            .build()
    }

    @Provides
    @Singleton
    fun provideRetrofit(client: OkHttpClient, moshi: Moshi): Retrofit {
        return Retrofit.Builder()
            .baseUrl("https://cosplay-planner.ru/")
            .client(client)
            .addConverterFactory(MoshiConverterFactory.create(moshi))
            .build()
    }

    @Provides
    @Singleton
    fun provideMobileApi(retrofit: Retrofit): MobileApi = retrofit.create(MobileApi::class.java)

    @Provides
    @Singleton
    fun provideDatabase(@ApplicationContext context: Context): AppDatabase {
        return Room.databaseBuilder(context, AppDatabase::class.java, "cosplay_mobile.db")
            .addMigrations(MIGRATION_2_3)
            .addMigrations(MIGRATION_3_4)
            .build()
    }

    private val MIGRATION_2_3 = object : Migration(2, 3) {
        override fun migrate(db: SupportSQLiteDatabase) {
            db.execSQL("ALTER TABLE festivals ADD COLUMN event_date TEXT")
            db.execSQL("ALTER TABLE festivals ADD COLUMN is_going INTEGER NOT NULL DEFAULT 0")
            db.execSQL("CREATE TABLE IF NOT EXISTS in_progress_cards (id INTEGER NOT NULL, card_id INTEGER NOT NULL, updated_at TEXT, payload_json TEXT NOT NULL, PRIMARY KEY(id))")
            db.execSQL("CREATE TABLE IF NOT EXISTS pigeon_messages (id INTEGER NOT NULL, chat_user_id INTEGER NOT NULL, chat_alias TEXT NOT NULL, direction TEXT NOT NULL, body TEXT NOT NULL, created_at TEXT, is_read INTEGER NOT NULL, PRIMARY KEY(id))")
        }
    }
    private val MIGRATION_3_4 = object : Migration(3, 4) {
        override fun migrate(db: SupportSQLiteDatabase) {
            db.execSQL("ALTER TABLE pigeon_messages ADD COLUMN emoji_json TEXT NOT NULL DEFAULT '{}'")
            db.execSQL("CREATE TABLE IF NOT EXISTS calendar_events (id INTEGER NOT NULL, date TEXT NOT NULL, time TEXT, title TEXT NOT NULL, city TEXT, details TEXT, updated_at TEXT, PRIMARY KEY(id))")
            db.execSQL("CREATE TABLE IF NOT EXISTS content_posts (id INTEGER NOT NULL, date TEXT NOT NULL, time TEXT, title TEXT NOT NULL, description TEXT, socials_json TEXT NOT NULL, rubric TEXT NOT NULL, status TEXT NOT NULL, published INTEGER NOT NULL, updated_at TEXT, PRIMARY KEY(id))")
        }
    }
}
