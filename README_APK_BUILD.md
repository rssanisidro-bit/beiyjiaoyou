# 相识北洋 Android APK 工程

这是把原始 Python 版封装成 Android APK 的工程。应用启动后会在后台运行 `main.py`，然后用 WebView 打开 `http://127.0.0.1:8765`。

## 构建要求

- JDK 17
- Android SDK
- Gradle 或 Android Studio
- 首次构建需要联网下载 Android Gradle Plugin 和 Chaquopy

## 构建命令

在本目录运行：

```powershell
gradle assembleDebug
```

成功后 APK 位于：

```text
app/build/outputs/apk/debug/app-debug.apk
```

## 用 GitHub Actions 自动构建

1. 在 GitHub 新建一个空仓库。
2. 把 `MeetBeiyangAndroid` 目录里的所有文件上传/推送到仓库根目录。
3. 打开仓库的 `Actions` 页面。
4. 选择 `Build Android APK`。
5. 点击 `Run workflow`，或直接推送到 `main` / `master` 后等待自动运行。
6. 构建成功后，在本次运行页面底部下载 `meet-beiyang-debug-apk`，里面就是 `app-debug.apk`。

## 说明

原 Python 程序使用 UDP 广播和 TCP 直连做局域网发现与聊天。Android 系统和不同校园网可能会限制广播、后台网络或设备互访；如果发现不了同学，请确认设备在同一 Wi-Fi、没有开启 AP 隔离，并允许应用联网。
