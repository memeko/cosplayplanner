package ru.cosplayplanner.mobile.data.remote

import android.content.Context
import okhttp3.Cookie
import okhttp3.CookieJar
import okhttp3.HttpUrl

class PersistentCookieJar(context: Context) : CookieJar {
    private val prefs = context.getSharedPreferences("mobile_session", Context.MODE_PRIVATE)

    override fun saveFromResponse(url: HttpUrl, cookies: List<Cookie>) {
        val stored = prefs.getStringSet("cookies", emptySet()).orEmpty().toMutableSet()
        cookies.forEach { fresh ->
            stored.removeAll { raw -> Cookie.parse(url, raw)?.let { it.name == fresh.name && it.domain == fresh.domain && it.path == fresh.path } == true }
            if (fresh.expiresAt > System.currentTimeMillis()) stored += fresh.toString()
        }
        prefs.edit().putStringSet("cookies", stored).apply()
    }

    override fun loadForRequest(url: HttpUrl): List<Cookie> {
        val now = System.currentTimeMillis()
        val parsed = prefs.getStringSet("cookies", emptySet()).orEmpty().mapNotNull { Cookie.parse(url, it) }
        val valid = parsed.filter { it.expiresAt > now && it.matches(url) }
        if (valid.size != parsed.size) prefs.edit().putStringSet("cookies", valid.map { it.toString() }.toSet()).apply()
        return valid
    }
}
