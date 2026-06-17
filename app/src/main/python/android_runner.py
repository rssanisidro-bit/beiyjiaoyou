import os
import sys
import threading

from com.chaquo.python import Python


def start():
    context = Python.getPlatform().getApplication()
    data_dir = context.getFilesDir().getAbsolutePath()
    os.chdir(data_dir)
    os.environ["BEIYANG_DATA_DIR"] = os.path.join(data_dir, "beiyang_data")

    def run_server():
        sys.argv = ["main.py", "--http-port", "8765", "--no-browser"]
        import main
        main.main()

    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()
