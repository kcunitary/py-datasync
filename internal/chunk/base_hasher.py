import abc
from typing import Any, Dict, AsyncGenerator

class hasher(abc.ABC):
    """协议适配器基类。

    通常，在 Adapter 中编写协议通信相关代码，如: 建立通信连接、处理接收与发送 data 等。

    参数:
        driver: {ref}`nonebot.drivers.Driver` 实例
        kwargs: 其他由 {ref}`nonebot.drivers.Driver.register_adapter` 传入的额外参数
    """

    def __init__(self, type: str):
        self.hasher: type



    def config(self):
        """全局 NoneBot 配置"""
        return self.driver.config