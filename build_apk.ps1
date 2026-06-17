$ErrorActionPreference = "Stop"

if (-not (Get-Command java -ErrorAction SilentlyContinue)) {
    throw "未找到 java。请先安装 JDK 17，或用 Android Studio 打开本工程。"
}

if (-not (Get-Command gradle -ErrorAction SilentlyContinue)) {
    throw "未找到 gradle。请先安装 Gradle，或用 Android Studio 打开本工程后执行 Build APK。"
}

gradle assembleDebug
