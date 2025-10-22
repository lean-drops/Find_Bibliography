#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import db_core as core
from db.overview.gui_main import launch_gui

def main() -> None:
    core.bootstrap()
    launch_gui(db_path=core.DB_PATH, lib_dir=core.LIB_DIR)

if __name__ == "__main__":
    main()