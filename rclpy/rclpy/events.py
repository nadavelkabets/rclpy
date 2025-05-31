from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from rclpy.executors import AbstractExecutor

__executor: Optional['AbstractExecutor'] = None

def get_executor() -> Optional['AbstractExecutor']:
    return __executor

def set_executor(executor: Optional['AbstractExecutor']):
    global __executor
    __executor = executor
