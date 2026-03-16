"""
睡眠设备 Modbus 控制工具库
将 Java 版本的 Modbus 指令生成和 TCP 通信功能移植到 Python
包含以下功能：
1. LearnModbusCG - 红外学习指令生成器
2. AirConditionerModbusCG - 空调控制指令生成器
3. TcpConnection - TCP 连接管理
4. EnhancedModbusClient - 增强版 Modbus 客户端
5. MqttModbusClient - MQTT 客户端（集成 Modbus 控制）

默认配置：
- TCP 默认连接: 192.168.10.153:50000
- MQTT Broker: broker.emqx.io:1883
- MQTT 订阅主题: sleepclaw/air

作者: 2pang
版权所有 (c) 2026
"""

import socket
import threading
import logging
import time
from enum import Enum
from typing import Dict, Optional, Callable, List
from dataclasses import dataclass, field

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 默认配置常量
DEFAULT_TCP_HOST = "192.168.10.153"
DEFAULT_TCP_PORT = 50000
DEFAULT_MQTT_BROKER = "broker.emqx.io"
DEFAULT_MQTT_PORT = 1883
DEFAULT_MQTT_TOPIC = "sleepclaw/air"


# ==================== CRC 计算工具 ====================

def calculate_crc(data: bytes) -> int:
    """
    计算 Modbus CRC16 校验码
    
    Args:
        data: 需要计算 CRC 的数据
        
    Returns:
        CRC16 校验码
    """
    crc = 0xFFFF  # 初始值
    
    for b in data:
        crc ^= (b & 0xFF)  # 转换为无符号字节
        
        for _ in range(8):
            if (crc & 0x0001) != 0:
                crc = (crc >> 1) ^ 0xA001  # 多项式反转
            else:
                crc = crc >> 1
    
    return crc


def bytes_to_hex(data: bytes) -> str:
    """将字节数组转换为十六进制字符串（大写）"""
    return data.hex().upper()


# ==================== LearnModbusCG - 红外学习指令生成器 ====================

class LearnModbusCG:
    """
    Modbus 协议指令生成器（红外学习指令）
    指令格式：设备地址（1字节）+ 功能码（固定06，1字节）+ 寄存器地址（固定0018，2字节）
            + 通道值（2字节）+ CRC校验码（2字节）
    """
    
    # 固定指令部分
    FUNCTION_CODE = 0x06  # 功能码固定为 0x06
    REGISTER_ADDRESS_HIGH = 0x00  # 寄存器地址高 8 位固定为 0x00
    REGISTER_ADDRESS_LOW = 0x18  # 寄存器地址低 8 位固定为 0x18
    
    @staticmethod
    def generate_command(device_address: int, channel: int) -> bytes:
        """
        生成红外学习指令
        
        Args:
            device_address: 设备地址 (1-255)
            channel: 通道号 (1-63)
            
        Returns:
            8 字节的指令字节数组
            
        Raises:
            ValueError: 当设备地址或通道号超出有效范围时
        """
        # 验证输入范围
        if not 1 <= device_address <= 255:
            raise ValueError("设备地址必须在 1-255 之间")
        if not 1 <= channel <= 63:
            raise ValueError("通道号必须在 1-63 之间")
        
        # 创建 6 字节的消息数组
        message = bytes([
            device_address,  # 设备地址
            LearnModbusCG.FUNCTION_CODE,  # 功能码
            LearnModbusCG.REGISTER_ADDRESS_HIGH,  # 寄存器地址高字节
            LearnModbusCG.REGISTER_ADDRESS_LOW,  # 寄存器地址低字节
            0x00,  # 通道值高字节（始终为 0）
            channel  # 通道值低字节
        ])
        
        # 计算 CRC 校验码
        crc = calculate_crc(message)
        crc_low = crc & 0xFF
        crc_high = (crc >> 8) & 0xFF
        
        # 组合完整指令
        full_command = message + bytes([crc_low, crc_high])
        
        return full_command


# ==================== AirConditionerModbusCG - 空调控制指令生成器 ====================

class Power(Enum):
    """开关机状态枚举"""
    OFF = ("关机", 0x0000, "0106002500009801")
    ON = ("开机", 0x0001, "01060025000159C1")
    
    def __init__(self, description, val, example_command):
        self.description = description
        self.val = val
        self.example_command = example_command


class FanSpeed(Enum):
    """风速枚举"""
    AUTO = ("自动风速", 0x0000, "01060028000009C2")
    SPEED_1 = ("1档", 0x0001, "010600280001C802")
    SPEED_2 = ("2档", 0x0002, "0106002800028803")
    SPEED_3 = ("3档", 0x0003, "01060028000349C3")
    
    def __init__(self, description, val, example_command):
        self.description = description
        self.val = val
        self.example_command = example_command


class Mode(Enum):
    """空调模式枚举"""
    AUTO = ("自动", 0x0000, "01060027000039C1")
    COOL = ("制冷", 0x0001, "010600270001F801")
    DEHUMIDIFY = ("除湿", 0x0002, "010600270002B800")
    HEAT = ("制热", 0x0004, "0106002700043802")
    FAN = ("送风", 0x0003, "01060027000379C0")
    
    def __init__(self, description, val, example_command):
        self.description = description
        self.val = val
        self.example_command = example_command


class Light(Enum):
    """灯光状态枚举"""
    OFF = ("关灯", 0x0000, "0106002900005802")
    ON = ("开灯", 0x0001, "01060029000199C2")
    
    def __init__(self, description, val, example_command):
        self.description = description
        self.val = val
        self.example_command = example_command


class AirConditionerModbusCG:
    """
    空调控制指令生成器（增强版）
    功能包括：开关机、风速控制、温度设置、模式切换、灯光控制
    """
    
    # 寄存器地址定义
    POWER_REGISTER = 0x0025  # 开关机寄存器地址
    TEMPERATURE_REGISTER = 0x0026  # 温度寄存器地址
    MODE_REGISTER = 0x0027  # 模式寄存器地址
    FAN_SPEED_REGISTER = 0x0028  # 风速寄存器地址
    LIGHT_REGISTER = 0x0029  # 灯光寄存器地址
    
    # 功能码（写单个寄存器）
    FUNCTION_CODE = 0x06
    
    def __init__(self, device_address: int = 1):
        """
        构造函数
        
        Args:
            device_address: 设备地址 (1-255)，默认为 1
            
        Raises:
            ValueError: 当设备地址超出有效范围时
        """
        self.set_device_address(device_address)
    
    def set_device_address(self, device_address: int):
        """
        设置设备地址
        
        Args:
            device_address: 设备地址 (1-255)
            
        Raises:
            ValueError: 当设备地址超出有效范围时
        """
        if not 1 <= device_address <= 255:
            raise ValueError("设备地址必须在 1-255 之间")
        self._device_address = device_address
    
    def get_device_address(self) -> int:
        """获取当前设备地址"""
        return self._device_address
    
    def _generate_command(self, register: int, value: int) -> bytes:
        """
        生成通用指令
        
        Args:
            register: 寄存器地址
            value: 写入的值
            
        Returns:
            8 字节的指令字节数组
        """
        # 创建 6 字节的消息数组
        message = bytes([
            self._device_address,  # 设备地址
            self.FUNCTION_CODE,  # 功能码
            (register >> 8) & 0xFF,  # 寄存器地址高字节
            register & 0xFF,  # 寄存器地址低字节
            (value >> 8) & 0xFF,  # 值高字节
            value & 0xFF  # 值低字节
        ])
        
        # 计算 CRC 校验码
        crc = calculate_crc(message)
        crc_low = crc & 0xFF
        crc_high = (crc >> 8) & 0xFF
        
        # 组合完整指令
        full_command = message + bytes([crc_low, crc_high])
        
        return full_command
    
    def generate_power_command(self, power: Power) -> bytes:
        """
        生成开关机指令
        
        Args:
            power: 开关机状态
            
        Returns:
            指令字节数组
        """
        return self._generate_command(self.POWER_REGISTER, power.val)
    
    def generate_fan_speed_command(self, fan_speed: FanSpeed) -> bytes:
        """
        生成风速控制指令
        
        Args:
            fan_speed: 风速设置
            
        Returns:
            指令字节数组
        """
        return self._generate_command(self.FAN_SPEED_REGISTER, fan_speed.val)
    
    def generate_temperature_command(self, temperature: int) -> bytes:
        """
        生成温度控制指令
        
        Args:
            temperature: 温度值 (16-30)
            
        Returns:
            指令字节数组
            
        Raises:
            ValueError: 当温度超出有效范围时
        """
        if not 16 <= temperature <= 30:
            raise ValueError("温度必须在 16-30 度之间")
        
        # 温度值转换：16 度对应 0x0000，17 度对应 0x0001，以此类推
        value = temperature - 16
        return self._generate_command(self.TEMPERATURE_REGISTER, value)
    
    def generate_mode_command(self, mode: Mode) -> bytes:
        """
        生成模式控制指令
        
        Args:
            mode: 空调模式
            
        Returns:
            指令字节数组
        """
        return self._generate_command(self.MODE_REGISTER, mode.val)
    
    def generate_light_command(self, light: Light) -> bytes:
        """
        生成灯光控制指令
        
        Args:
            light: 灯光状态
            
        Returns:
            指令字节数组
        """
        return self._generate_command(self.LIGHT_REGISTER, light.val)
    
    def generate_complete_settings(
        self,
        power: Power,
        temperature: int,
        mode: Mode,
        fan_speed: FanSpeed,
        light: Light
    ) -> Dict[str, bytes]:
        """
        生成完整空调设置指令（一键设置所有参数）
        
        Args:
            power: 开关机状态
            temperature: 温度 (16-30)
            mode: 模式
            fan_speed: 风速
            light: 灯光
            
        Returns:
            包含所有指令的字典
        """
        return {
            "power": self.generate_power_command(power),
            "temperature": self.generate_temperature_command(temperature),
            "mode": self.generate_mode_command(mode),
            "fanSpeed": self.generate_fan_speed_command(fan_speed),
            "light": self.generate_light_command(light)
        }


# ==================== TcpConnection - TCP 连接管理 ====================

@dataclass
class TcpConnection:
    """TCP 连接管理类"""
    
    server_id: str
    host: str
    port: int
    description: str = field(default="")
    _socket: Optional[socket.socket] = field(default=None, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    
    CONNECTION_TIMEOUT = 3  # 3 秒连接超时
    
    def __post_init__(self):
        if not self.description:
            self.description = self.server_id
    
    def ensure_connected(self) -> bool:
        """
        确保连接有效
        
        Returns:
            True 如果连接成功，False 如果连接失败
        """
        with self._lock:
            if self._socket is not None:
                try:
                    # 检查连接是否仍然有效
                    self._socket.getpeername()
                    logger.info(f"TCP 连接已存在且有效: {self.server_id}")
                    return True
                except (OSError, socket.error) as e:
                    # 连接已失效，关闭它
                    logger.warning(f"TCP 连接已失效: {e}")
                    self.close()
            
            try:
                logger.info(f"正在建立 TCP 连接: {self.host}:{self.port}")
                self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._socket.settimeout(self.CONNECTION_TIMEOUT)
                self._socket.connect((self.host, self.port))
                
                logger.info(f"已连接到服务端: {self.server_id} - {self.host}:{self.port}")
                return True
            except (socket.error, OSError) as e:
                logger.error(f"连接服务端 {self.description} 失败: {e}")
                self._socket = None
                return False
    
    def close(self):
        """关闭连接"""
        with self._lock:
            if self._socket is not None:
                try:
                    self._socket.close()
                    logger.info(f"已关闭服务端连接: {self.description}")
                except (socket.error, OSError) as e:
                    logger.error(f"关闭连接时出错: {e}")
                finally:
                    self._socket = None
    
    @property
    def socket(self) -> Optional[socket.socket]:
        """获取 socket 对象"""
        return self._socket
    
    def is_connected(self) -> bool:
        """检查是否已连接"""
        if self._socket is None:
            return False
        try:
            self._socket.getpeername()
            return True
        except (OSError, socket.error):
            return False


# ==================== EnhancedModbusClient - 增强版 Modbus 客户端 ====================

class ResponseValidators:
    """预定义的响应验证器"""
    
    @staticmethod
    def length_validator(response: bytes) -> bool:
        """简单验证器：检查响应长度至少为 8 字节"""
        return len(response) >= 8
    
    @staticmethod
    def crc_validator(response: bytes) -> bool:
        """CRC 验证器：验证响应数据的 CRC 校验码"""
        if len(response) < 8:
            return False
        
        # 提取数据和 CRC
        data = response[:6]
        received_crc = ((response[7] & 0xFF) << 8) | (response[6] & 0xFF)
        
        # 计算 CRC
        calculated_crc = calculate_crc(data)
        return received_crc == calculated_crc
    
    @staticmethod
    def full_validator(response: bytes) -> bool:
        """完整验证器：检查功能码和 CRC"""
        if len(response) < 8:
            return False
        
        # 检查功能码是否正确（应该是 0x06）
        if (response[1] & 0xFF) != 0x06:
            return False
        
        # 验证 CRC
        return ResponseValidators.crc_validator(response)
    
    @staticmethod
    def create_echo_validator(request: bytes) -> Callable[[bytes], bool]:
        """
        回显验证器：检查响应是否与请求相同
        
        Args:
            request: 请求数据
            
        Returns:
            验证器函数
        """
        def validator(response: bytes) -> bool:
            return response == request
        return validator


class EnhancedModbusClient:
    """
    增强版 Modbus 客户端
    支持多服务端管理、自动重连、响应验证等功能
    """
    
    RESPONSE_TIMEOUT = 3  # 3 秒响应超时
    MAX_RESPONSE_SIZE = 256  # 最大响应长度
    
    def __init__(self):
        """初始化客户端"""
        # 存储服务端配置和连接
        self._connections: Dict[str, TcpConnection] = {}
        self._lock = threading.Lock()
    
    def add_or_update_server(self, server_id: str, host: str, port: int, description: str = ""):
        """
        添加或更新服务端配置
        
        Args:
            server_id: 服务端唯一标识
            host: 服务端地址
            port: 服务端端口
            description: 服务端描述
        """
        with self._lock:
            conn = self._connections.get(server_id)
            if conn is None:
                # 创建新连接
                conn = TcpConnection(server_id, host, port, description)
                self._connections[server_id] = conn
                logger.info(f"添加新服务端: {server_id} - {host}:{port}")
            else:
                # 更新现有连接
                if conn.host != host or conn.port != port:
                    conn.close()  # 关闭旧连接
                    conn.host = host
                    conn.port = port
                    conn.description = description or server_id
                    logger.info(f"更新服务端配置: {server_id} - {host}:{port}")
    
    def remove_server(self, server_id: str):
        """
        移除服务端配置
        
        Args:
            server_id: 服务端唯一标识
        """
        with self._lock:
            conn = self._connections.pop(server_id, None)
            if conn is not None:
                conn.close()
                logger.info(f"移除服务端: {server_id}")
    
    def test_connection(self, server_id: str, host: str, port: int, description: str = "") -> bool:
        """
        测试服务端连接是否可用
        
        Args:
            server_id: 服务端唯一标识
            host: 服务端地址
            port: 服务端端口
            description: 服务端描述
            
        Returns:
            True 连接成功，False 连接失败
        """
        try:
            # 添加或更新服务端配置
            self.add_or_update_server(server_id, host, port, description)
            
            # 获取连接并测试
            conn = self._connections.get(server_id)
            if conn is None:
                return False
            
            # 直接调用 ensure_connected 建立连接（内部会处理旧连接）
            return conn.ensure_connected()
        except Exception as e:
            logger.error(f"测试连接失败: serverId={server_id}, host={host}, port={port}, error={e}")
            return False
    
    def send_command_with_response(self, server_id: str, command: bytes) -> Optional[bytes]:
        """
        发送指令到指定服务端并获取响应
        
        Args:
            server_id: 服务端唯一标识
            command: 指令字节数组
            
        Returns:
            响应数据字节数组，如果失败返回 None
            
        Raises:
            RuntimeError: 当未找到服务端配置或无法连接时
        """
        conn = self._connections.get(server_id)
        if conn is None:
            logger.error(f"未找到服务端配置: {server_id}")
            raise RuntimeError(f"未找到服务端配置: {server_id}")
        
        # 确保连接有效
        if not conn.ensure_connected():
            logger.error(f"无法连接到服务端: {server_id}")
            raise RuntimeError(f"无法连接到服务端[{conn.description}]: {conn.host}:{conn.port}")
        
        try:
            sock = conn.socket
            if sock is None:
                raise RuntimeError("Socket 为空")
            
            # 发送指令
            sock.sendall(command)
            
            # 读取响应
            return self._read_response(sock)
        except (socket.error, OSError) as e:
            logger.error(f"发送指令到 {conn.description} 失败: {e}")
            # 关闭问题连接
            conn.close()
            return None
        except Exception as e:
            logger.error(f"发送指令时发生错误: {e}")
            return None
    
    def send_and_validate(
        self,
        server_id: str,
        command: bytes,
        validator: Callable[[bytes], bool]
    ) -> bool:
        """
        发送指令并验证响应
        
        Args:
            server_id: 服务端唯一标识
            command: 指令字节数组
            validator: 响应验证器函数
            
        Returns:
            是否发送成功且响应验证通过
        """
        try:
            response = self.send_command_with_response(server_id, command)
            if response is None:
                return False
            return validator(response)
        except RuntimeError:
            return False
    
    def _read_response(self, sock: socket.socket) -> bytes:
        """
        读取服务端响应
        
        Args:
            sock: socket 对象
            
        Returns:
            响应数据字节数组
            
        Raises:
            socket.timeout: 当读取超时时
            socket.error: 当发生 socket 错误时
        """
        sock.settimeout(self.RESPONSE_TIMEOUT)
        
        buffer = bytearray()
        
        try:
            while len(buffer) < self.MAX_RESPONSE_SIZE:
                try:
                    chunk = sock.recv(self.MAX_RESPONSE_SIZE - len(buffer))
                    if not chunk:
                        # 连接已关闭
                        break
                    buffer.extend(chunk)
                    
                    # 检查是否已读取完整响应
                    if self._is_complete_response(buffer):
                        break
                except socket.timeout:
                    # 超时，返回已接收的数据
                    break
        except socket.error:
            pass
        
        if len(buffer) == 0:
            raise socket.error("未收到任何响应")
        
        return bytes(buffer)
    
    def _is_complete_response(self, buffer) -> bool:
        """
        检查是否已收到完整响应
        
        默认实现：检查响应长度至少为 8 字节（Modbus RTU 最小响应长度）
        可根据实际协议扩展此方法
        
        Args:
            buffer: 已接收的数据缓冲区 (bytes 或 bytearray)
            
        Returns:
            是否已收到完整响应
        """
        # 简单实现：至少需要 8 字节响应
        return len(buffer) >= 8
    
    def shutdown(self):
        """关闭所有连接"""
        with self._lock:
            logger.info("关闭所有 TCP 连接...")
            for conn in self._connections.values():
                conn.close()
            self._connections.clear()
    
    def get_server_ids(self) -> List[str]:
        """获取所有服务端 ID"""
        with self._lock:
            return list(self._connections.keys())
    
    def get_connection(self, server_id: str) -> Optional[TcpConnection]:
        """获取指定服务端的连接信息"""
        return self._connections.get(server_id)


# ==================== 便捷函数 ====================

def create_learn_command(device_address: int, channel: int) -> bytes:
    """
    便捷函数：生成红外学习指令
    
    Args:
        device_address: 设备地址 (1-255)
        channel: 通道号 (1-63)
        
    Returns:
        8 字节的指令字节数组
    """
    return LearnModbusCG.generate_command(device_address, channel)


def create_ac_command(
    device_address: int = 1,
    power: Optional[Power] = None,
    temperature: Optional[int] = None,
    mode: Optional[Mode] = None,
    fan_speed: Optional[FanSpeed] = None,
    light: Optional[Light] = None
) -> Dict[str, bytes]:
    """
    便捷函数：生成空调控制指令
    
    Args:
        device_address: 设备地址 (1-255)，默认为 1
        power: 开关机状态
        temperature: 温度值 (16-30)
        mode: 空调模式
        fan_speed: 风速
        light: 灯光状态
        
    Returns:
        包含生成的指令的字典
    """
    generator = AirConditionerModbusCG(device_address)
    commands = {}
    
    if power is not None:
        commands['power'] = generator.generate_power_command(power)
    if temperature is not None:
        commands['temperature'] = generator.generate_temperature_command(temperature)
    if mode is not None:
        commands['mode'] = generator.generate_mode_command(mode)
    if fan_speed is not None:
        commands['fan_speed'] = generator.generate_fan_speed_command(fan_speed)
    if light is not None:
        commands['light'] = generator.generate_light_command(light)
    
    return commands


# ==================== MQTT 客户端（集成 Modbus 控制）====================

# 尝试导入 paho-mqtt，如果失败则记录警告
mqtt = None  # type: ignore
PAHO_MQTT_AVAILABLE = False
try:
    import paho.mqtt.client as mqtt
    PAHO_MQTT_AVAILABLE = True
except ImportError:
    logger.warning("paho-mqtt 库未安装，MQTT 功能不可用。请运行: pip install paho-mqtt")


class MqttModbusClient:
    """
    MQTT 客户端（集成 Modbus 控制）
    
    功能：
    - 连接到 MQTT Broker
    - 订阅指定主题
    - 当收到消息为 "on" 时，通过 TCP 发送空调开机指令
    - 当收到消息为 "off" 时，通过 TCP 发送空调关机指令
    
    默认配置：
    - TCP 连接: 192.168.10.153:50000
    - MQTT Broker: broker.emqx.io:1883
    - MQTT 主题: sleepclaw/air
    """
    
    def __init__(
        self,
        tcp_host: str = DEFAULT_TCP_HOST,
        tcp_port: int = DEFAULT_TCP_PORT,
        mqtt_broker: str = DEFAULT_MQTT_BROKER,
        mqtt_port: int = DEFAULT_MQTT_PORT,
        mqtt_topic: str = DEFAULT_MQTT_TOPIC,
        device_address: int = 1,
        client_id: Optional[str] = None
    ):
        """
        初始化 MQTT Modbus 客户端
        
        Args:
            tcp_host: TCP 服务端地址，默认 192.168.10.153
            tcp_port: TCP 服务端端口，默认 50000
            mqtt_broker: MQTT Broker 地址，默认 broker.emqx.io
            mqtt_port: MQTT 端口，默认 1883
            mqtt_topic: MQTT 订阅主题，默认 sleepclaw/air
            device_address: 空调设备地址，默认 1
            client_id: MQTT 客户端 ID，默认自动生成
        """
        if not PAHO_MQTT_AVAILABLE:
            raise ImportError("paho-mqtt 库未安装，请运行: pip install paho-mqtt")
        
        self.tcp_host = tcp_host
        self.tcp_port = tcp_port
        self.mqtt_broker = mqtt_broker
        self.mqtt_port = mqtt_port
        self.mqtt_topic = mqtt_topic
        self.device_address = device_address
        
        # 创建 Modbus 客户端
        self.modbus_client = EnhancedModbusClient()
        self.modbus_client.add_or_update_server(
            "default", tcp_host, tcp_port, "空调控制器"
        )
        
        # 创建空调指令生成器
        self.ac_generator = AirConditionerModbusCG(device_address)
        
        # 创建 MQTT 客户端
        self.client_id = client_id or f"sleepclaw_client_{int(time.time())}"
        self.mqtt_client = mqtt.Client(client_id=self.client_id)
        
        # 设置回调函数
        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_message = self._on_message
        self.mqtt_client.on_disconnect = self._on_disconnect
        
        self._connected = False
        self._running = False
        self._thread: Optional[threading.Thread] = None
    
    def _on_connect(self, client, userdata, flags, rc):
        """MQTT 连接回调"""
        if rc == 0:
            self._connected = True
            logger.info(f"已连接到 MQTT Broker: {self.mqtt_broker}:{self.mqtt_port}")
            # 订阅主题
            client.subscribe(self.mqtt_topic)
            logger.info(f"已订阅主题: {self.mqtt_topic}")
        else:
            logger.error(f"MQTT 连接失败，返回码: {rc}")
    
    def _on_disconnect(self, client, userdata, rc):
        """MQTT 断开连接回调"""
        self._connected = False
        if rc != 0:
            logger.warning(f"MQTT 意外断开，返回码: {rc}，尝试重连...")
    
    def _on_message(self, client, userdata, msg):
        """MQTT 消息接收回调"""
        try:
            payload = msg.payload.decode('utf-8').strip().lower()
            logger.info(f"收到 MQTT 消息 - 主题: {msg.topic}, 内容: {payload}")
            
            # 根据消息内容控制空调
            if payload == "on":
                self._send_ac_power_command(Power.ON)
            elif payload == "off":
                self._send_ac_power_command(Power.OFF)
            elif payload == "toggle":
                # 切换开关状态（可通过状态追踪实现）
                logger.info("切换空调状态")
                self._send_ac_power_command(Power.ON)
            else:
                logger.warning(f"未知的消息内容: {payload}")
                
        except Exception as e:
            logger.error(f"处理 MQTT 消息时出错: {e}")
    
    def _send_ac_power_command(self, power: Power):
        """
        发送空调开关指令
        
        Args:
            power: 开关机状态
        """
        try:
            command = self.ac_generator.generate_power_command(power)
            logger.info(f"准备发送空调{'开机' if power == Power.ON else '关机'}指令: {bytes_to_hex(command)}")
            
            # 先检查 TCP 连接状态
            conn = self.modbus_client.get_connection("default")
            if conn:
                logger.info(f"TCP 连接状态（发送前）: host={conn.host}, port={conn.port}, connected={conn.is_connected()}")
            else:
                logger.warning("TCP 连接配置不存在，尝试添加...")
                self.modbus_client.add_or_update_server("default", self.tcp_host, self.tcp_port, "空调控制器")
                conn = self.modbus_client.get_connection("default")
            
            # 确保连接建立
            if conn and not conn.is_connected():
                logger.info("TCP 未连接，尝试建立连接...")
                if not conn.ensure_connected():
                    logger.error("TCP 连接建立失败")
                    return
            
            response = self.modbus_client.send_command_with_response("default", command)
            
            # 再次检查连接状态
            if conn:
                logger.info(f"TCP 连接状态（发送后）: connected={conn.is_connected()}")
            
            if response:
                logger.info(f"收到响应: {bytes_to_hex(response)}")
                # 验证响应
                is_valid = ResponseValidators.full_validator(response)
                logger.info(f"响应验证: {'成功' if is_valid else '失败'}")
            else:
                logger.error("未收到响应，TCP 连接可能失败")
                
        except Exception as e:
            logger.error(f"发送空调指令时出错: {e}", exc_info=True)
    
    def connect(self, timeout: int = 10) -> bool:
        """
        连接到 MQTT Broker
        
        Args:
            timeout: 连接超时时间（秒）
            
        Returns:
            是否连接成功
        """
        try:
            logger.info(f"正在连接 MQTT Broker: {self.mqtt_broker}:{self.mqtt_port}")
            self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, 60)
            
            # 启动网络循环
            self.mqtt_client.loop_start()
            
            # 等待连接成功
            start_time = time.time()
            while not self._connected and time.time() - start_time < timeout:
                time.sleep(0.1)
            
            if self._connected:
                logger.info("MQTT 连接成功")
                return True
            else:
                logger.error("MQTT 连接超时")
                return False
                
        except Exception as e:
            logger.error(f"MQTT 连接失败: {e}")
            return False
    
    def disconnect(self):
        """断开 MQTT 连接"""
        self._running = False
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()
        self.modbus_client.shutdown()
        logger.info("MQTT 客户端已断开")
    
    def start_blocking(self):
        """以阻塞方式运行 MQTT 客户端"""
        try:
            if self.connect():
                logger.info("MQTT 客户端正在运行，按 Ctrl+C 停止...")
                self._running = True
                while self._running:
                    time.sleep(1)
        except KeyboardInterrupt:
            logger.info("收到停止信号")
        finally:
            self.disconnect()
    
    def publish(self, topic: str, message: str, qos: int = 0) -> bool:
        """
        发布 MQTT 消息
        
        Args:
            topic: 主题
            message: 消息内容
            qos: 服务质量等级
            
        Returns:
            是否发布成功
        """
        if not self._connected:
            logger.error("MQTT 未连接，无法发布消息")
            return False
        
        result = self.mqtt_client.publish(topic, message, qos)
        # MQTT_ERR_SUCCESS = 0
        return hasattr(result, 'rc') and result.rc == 0
    
    def is_connected(self) -> bool:
        """检查 MQTT 是否已连接"""
        return self._connected


# ==================== 便捷函数 ====================

def create_default_mqtt_client(device_address: int = 1) -> MqttModbusClient:
    """
    创建使用默认配置的 MQTT 客户端
    
    默认配置：
    - TCP: 192.168.10.153:50000
    - MQTT: broker.emqx.io:1883
    - 主题: sleepclaw/air
    
    Args:
        device_address: 空调设备地址，默认 1
        
    Returns:
        MqttModbusClient 实例
    """
    return MqttModbusClient(device_address=device_address)


# ==================== 主程序入口 ====================

def run_client():
    """
    启动 MQTT 客户端 - 空调控制器
    持续运行，监听 MQTT 消息并控制空调
    """
    print("=" * 60)
    print("启动 MQTT 客户端 - 空调控制器")
    print("=" * 60)
    print()
    print("配置信息:")
    print(f"  TCP 连接: {DEFAULT_TCP_HOST}:{DEFAULT_TCP_PORT}")
    print(f"  MQTT Broker: {DEFAULT_MQTT_BROKER}:{DEFAULT_MQTT_PORT}")
    print(f"  订阅主题: {DEFAULT_MQTT_TOPIC}")
    print("  设备地址: 1")
    print()
    print("等待 MQTT 消息...")
    print(f"  发送 'on'  到主题 {DEFAULT_MQTT_TOPIC} → 空调开机")
    print(f"  发送 'off' 到主题 {DEFAULT_MQTT_TOPIC} → 空调关机")
    print()
    print("按 Ctrl+C 停止程序")
    print("=" * 60)
    print()

    # 创建客户端并启动（使用默认配置）
    client = create_default_mqtt_client(device_address=1)
    
    # 先测试 TCP 连接
    print(f"正在测试 TCP 连接到 {DEFAULT_TCP_HOST}:{DEFAULT_TCP_PORT}...")
    
    # 直接使用 socket 测试连接
    test_sock = None
    try:
        test_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        test_sock.settimeout(3)
        test_sock.connect((DEFAULT_TCP_HOST, DEFAULT_TCP_PORT))
        print("TCP 连接测试成功！")
        tcp_ok = True
    except Exception as e:
        print(f"TCP 连接测试失败: {e}")
        print("警告: 请检查：")
        print(f"  1. 设备是否已开机并连接到网络")
        print(f"  2. IP 地址 {DEFAULT_TCP_HOST} 是否正确")
        print(f"  3. 端口 {DEFAULT_TCP_PORT} 是否正确")
        print(f"  4. 网络是否通畅（尝试 ping {DEFAULT_TCP_HOST}）")
        print()
        print("程序将继续运行，但可能无法正常控制空调")
        print()
        tcp_ok = False
    finally:
        if test_sock:
            try:
                test_sock.close()
            except:
                pass

    try:
        client.start_blocking()
    except Exception as e:
        print(f"程序异常: {e}")


def run_tests():
    """运行测试"""
    print("=" * 60)
    print("睡眠设备 Modbus 控制工具库 - 测试")
    print("=" * 60)
    
    # 测试 LearnModbusCG
    print("\n=== 测试 LearnModbusCG（红外学习指令生成器）===")
    print(f"地址 1, 通道 1: {bytes_to_hex(LearnModbusCG.generate_command(1, 1))}")  # 010600180001C80D
    print(f"地址 1, 通道 2: {bytes_to_hex(LearnModbusCG.generate_command(1, 2))}")  # 010600180002880C
    print(f"地址 1, 通道 63: {bytes_to_hex(LearnModbusCG.generate_command(1, 63))}")  # 01060018003F49DD
    print(f"地址 255, 通道 1: {bytes_to_hex(LearnModbusCG.generate_command(255, 1))}")  # FF0600180001DDD3
    print(f"地址 255, 通道 63: {bytes_to_hex(LearnModbusCG.generate_command(255, 63))}")  # FF060018003F5C03
    
    # 测试 AirConditionerModbusCG
    print("\n=== 测试 AirConditionerModbusCG（空调控制指令生成器）===")
    ac_generator = AirConditionerModbusCG(185)
    
    print("\n开关机控制:")
    for power in Power:
        command = ac_generator.generate_power_command(power)
        print(f"  {power.description}: {bytes_to_hex(command)}")
    
    print("\n风速控制:")
    for speed in FanSpeed:
        command = ac_generator.generate_fan_speed_command(speed)
        print(f"  {speed.description}: {bytes_to_hex(command)}")
    
    print("\n温度控制:")
    for temp in range(16, 31):
        command = ac_generator.generate_temperature_command(temp)
        print(f"  {temp}℃: {bytes_to_hex(command)}")
    
    print("\n模式控制:")
    for mode in Mode:
        command = ac_generator.generate_mode_command(mode)
        print(f"  {mode.description}: {bytes_to_hex(command)}")
    
    print("\n灯光控制:")
    for light in Light:
        command = ac_generator.generate_light_command(light)
        print(f"  {light.description}: {bytes_to_hex(command)}")
    
    print("\n完整空调设置示例:")
    complete_settings = ac_generator.generate_complete_settings(
        Power.ON,  # 开机
        25,  # 25℃
        Mode.COOL,  # 制冷模式
        FanSpeed.SPEED_1,  # 1 档风速
        Light.OFF  # 关灯
    )
    for key, value in complete_settings.items():
        print(f"  {key}: {bytes_to_hex(value)}")
    
    # 演示使用不同设备地址
    print("\n使用设备地址 2 生成指令:")
    ac_generator.set_device_address(2)
    command = ac_generator.generate_power_command(Power.ON)
    print(f"设备地址 2，开机指令: {bytes_to_hex(command)}")
    
    # 测试便捷函数
    print("\n=== 测试便捷函数 ===")
    cmd = create_learn_command(1, 1)
    print(f"便捷函数生成学习指令: {bytes_to_hex(cmd)}")
    
    ac_cmds = create_ac_command(
        device_address=1,
        power=Power.ON,
        temperature=26,
        mode=Mode.COOL
    )
    print(f"便捷函数生成空调指令: { {k: bytes_to_hex(v) for k, v in ac_cmds.items()} }")
    
    # 测试 CRC 计算
    print("\n=== 测试 CRC 计算 ===")
    test_data = bytes([0x01, 0x06, 0x00, 0x18, 0x00, 0x01])
    crc = calculate_crc(test_data)
    print(f"数据 {bytes_to_hex(test_data)} 的 CRC: {crc:04X}")
    
    # 测试默认配置
    print("\n=== 默认配置 ===")
    print(f"默认 TCP 地址: {DEFAULT_TCP_HOST}:{DEFAULT_TCP_PORT}")
    print(f"默认 MQTT Broker: {DEFAULT_MQTT_BROKER}:{DEFAULT_MQTT_PORT}")
    print(f"默认 MQTT 主题: {DEFAULT_MQTT_TOPIC}")
    
    # MQTT 功能演示（需要安装 paho-mqtt）
    print("\n=== MQTT 客户端演示 ===")
    if PAHO_MQTT_AVAILABLE:
        print("paho-mqtt 已安装，可以使用 MQTT 功能")
        print("\n使用示例:")
        print("  from sleep_utils import create_default_mqtt_client")
        print("  client = create_default_mqtt_client(device_address=1)")
        print("  client.start_blocking()  # 启动并阻塞运行")
        print("\n或者:")
        print("  from sleep_utils import MqttModbusClient")
        print("  client = MqttModbusClient(")
        print("      tcp_host='192.168.10.153',")
        print("      tcp_port=50000,")
        print("      mqtt_broker='broker.emqx.io',")
        print("      mqtt_port=1883,")
        print("      mqtt_topic='sleepclaw/air',")
        print("      device_address=1")
        print("  )")
        print("  client.start_blocking()")
        print("\n向主题 'sleepclaw/air' 发送 'on' 将打开空调")
        print("向主题 'sleepclaw/air' 发送 'off' 将关闭空调")
    else:
        print("paho-mqtt 未安装，MQTT 功能不可用")
        print("请运行: pip install paho-mqtt")
    
    print("\n" + "=" * 60)
    print("测试完成！")
    print("=" * 60)


if __name__ == "__main__":
    import sys
    
    # 如果有参数 --test，则运行测试；否则运行客户端
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        run_tests()
    else:
        run_client()
