from pathlib import Path
from vnpy.trader.app import BaseApp
from .engine import OptionEngine, APP_NAME


class OptionMasterApp(BaseApp):
    """"""
    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "ζζδΊ€ζ"
    engine_class = OptionEngine
    widget_name = "OptionManager"
    icon_name = "option.ico"
