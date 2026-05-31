#!/usr/bin/env python3
"""
局域网命令行聊天工具
支持联系人管理、离线消息、消息持久化
"""
import socket
import threading
import json
import os
import time
from datetime import datetime
from collections import defaultdict

# ==================== 配置 ====================
PORT = 12345
DATA_DIR = os.path.expanduser('~/.lanchat')
CONTACTS_FILE = os.path.join(DATA_DIR, 'contacts.json')
MESSAGES_FILE = os.path.join(DATA_DIR, 'messages.json')
PENDING_FILE = os.path.join(DATA_DIR, 'pending.json')

# ==================== 数据管理 ====================
class DataManager:
    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.contacts = self.load_json(CONTACTS_FILE, {})  # {ip: 备注}
        self.messages = self.load_json(MESSAGES_FILE, {})  # {ip: [{from, content, time, read}]}
        self.pending = self.load_json(PENDING_FILE, {})    # {ip: [{content, time}]}
        self.unread_count = defaultdict(int)
        self.lock = threading.Lock()
        
        # 计算未读消息数
        self.recount_unread()
    
    def load_json(self, filepath, default):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return default
    
    def save_json(self, filepath, data):
        with self.lock:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
    
    def save_all(self):
        self.save_json(CONTACTS_FILE, self.contacts)
        self.save_json(MESSAGES_FILE, self.messages)
        self.save_json(PENDING_FILE, self.pending)
    
    def get_remark(self, ip):
        return self.contacts.get(ip, ip)
    
    def get_ip_by_remark(self, remark):
        for ip, r in self.contacts.items():
            if r == remark:
                return ip
        return None
    
    def add_contact(self, ip, remark=None):
        self.contacts[ip] = remark or ip
        if ip not in self.messages:
            self.messages[ip] = []
        if ip not in self.pending:
            self.pending[ip] = []
        self.save_all()
    
    def add_message(self, ip, from_ip, content, read=False):
        if ip not in self.messages:
            self.messages[ip] = []
        msg = {
            'from': from_ip,
            'content': content,
            'time': datetime.now().strftime('%H:%M:%S'),
            'read': read
        }
        self.messages[ip].append(msg)
        if not read:
            self.unread_count[ip] += 1
        self.save_json(MESSAGES_FILE, self.messages)
    
    def add_pending(self, target_ip, content):
        if target_ip not in self.pending:
            self.pending[target_ip] = []
        self.pending[target_ip].append({
            'content': content,
            'time': datetime.now().strftime('%H:%M:%S')
        })
        self.save_json(PENDING_FILE, self.pending)
    
    def get_unread_messages(self, ip):
        if ip not in self.messages:
            return []
        unread = [m for m in self.messages[ip] if not m['read']]
        # 标记为已读
        for m in unread:
            m['read'] = True
        self.unread_count[ip] = 0
        self.save_json(MESSAGES_FILE, self.messages)
        return unread
    
    def recount_unread(self):
        self.unread_count.clear()
        for ip, msgs in self.messages.items():
            self.unread_count[ip] = sum(1 for m in msgs if not m['read'])
    
    def list_contacts(self):
        result = []
        for ip, remark in self.contacts.items():
            unread = self.unread_count.get(ip, 0)
            pending = len(self.pending.get(ip, []))
            result.append((ip, remark, unread, pending))
        return result

# ==================== 网络层 ====================
class NetworkManager:
    def __init__(self, data_manager):
        self.data = data_manager
        self.running = True
        self.online_contacts = set()
        self.server_socket = None
        
    def get_my_ip(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('8.8.8.8', 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except:
            return '127.0.0.1'
    
    def start_server(self):
        """启动接收服务器"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('0.0.0.0', PORT))
        self.server_socket.listen(5)
        self.server_socket.settimeout(1)
        
        def listen_loop():
            while self.running:
                try:
                    conn, addr = self.server_socket.accept()
                    ip = addr[0]
                    threading.Thread(target=self.handle_connection, 
                                   args=(conn, ip), daemon=True).start()
                except socket.timeout:
                    continue
                except:
                    break
        
        threading.Thread(target=listen_loop, daemon=True).start()
    
    def handle_connection(self, conn, ip):
        """处理进入的连接"""
        try:
            # 接收消息长度
            size_data = conn.recv(8)
            if not size_data:
                return
            size = int.from_bytes(size_data, 'big')
            
            # 接收消息内容
            data = b''
            while len(data) < size:
                chunk = conn.recv(min(size - len(data), 4096))
                if not chunk:
                    break
                data += chunk
            
            msg = json.loads(data.decode('utf-8'))
            content = msg['content']
            
            # 确保发送者在联系人列表中
            if ip not in self.data.contacts:
                self.data.add_contact(ip)
            
            # 保存消息
            self.data.add_message(ip, ip, content, read=False)
            
            # 发送确认
            conn.send(b'OK')
            
        except Exception as e:
            pass
        finally:
            conn.close()
    
    def send_message(self, target_ip, content):
        """发送消息到指定IP"""
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.settimeout(3)
            client.connect((target_ip, PORT))
            
            msg = json.dumps({'content': content})
            data = msg.encode('utf-8')
            size = len(data).to_bytes(8, 'big')
            
            client.send(size + data)
            
            # 等待确认
            response = client.recv(2)
            client.close()
            
            if response == b'OK':
                return True, "发送成功"
            return False, "未收到确认"
            
        except socket.timeout:
            return False, "连接超时，对方可能不在线"
        except ConnectionRefusedError:
            return False, "连接被拒绝，对方可能不在线"
        except Exception as e:
            return False, f"发送失败: {e}"
    
    def check_online(self, ip):
        """检测对方是否在线"""
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.settimeout(1)
            client.connect((ip, PORT))
            client.close()
            return True
        except:
            return False
    
    def retry_pending_messages(self):
        """重试发送离线消息"""
        while self.running:
            for ip in list(self.data.pending.keys()):
                if self.data.pending[ip] and self.check_online(ip):
                    pending_msgs = self.data.pending[ip][:]
                    for msg in pending_msgs:
                        success, _ = self.send_message(ip, msg['content'])
                        if success:
                            self.data.pending[ip].remove(msg)
                            # 同时保存到已发送消息
                            self.data.add_message(ip, self.get_my_ip(), 
                                                msg['content'], read=True)
                            self.data.save_all()
            time.sleep(5)  # 每5秒检查一次
    
    def stop(self):
        self.running = False
        if self.server_socket:
            self.server_socket.close()

# ==================== 界面工具 ====================
def wrap_text(text, max_width=40, indent=20):
    """自动换行，保持右对齐"""
    words = list(text)
    lines = []
    current_line = ''
    
    for char in words:
        if len(current_line) < max_width:
            current_line += char
        else:
            lines.append(current_line)
            current_line = char
    
    if current_line:
        lines.append(current_line)
    
    return lines

def format_sent_message(content, time_str):
    """格式化已发送消息 - 右对齐"""
    max_width = 50
    lines = wrap_text(content, max_width)
    
    output = []
    # 最后一行带时间
    for i, line in enumerate(lines):
        if i == len(lines) - 1:
            formatted = f"{line} [{time_str}]"
        else:
            formatted = line
        # 右对齐（终端宽度假设70）
        output.append(formatted.rjust(70))
    
    return '\n'.join(output)

def clear_screen():
    os.system('clear' if os.name != 'nt' else 'cls')

# ==================== 主程序 ====================
class ChatApp:
    def __init__(self):
        self.data = DataManager()
        self.network = NetworkManager(self.data)
        self.my_ip = self.network.get_my_ip()
        self.current_chat = None  # 当前聊天对象的IP
        self.chat_mode = False
        
    def start(self):
        """启动应用"""
        self.network.start_server()
        threading.Thread(target=self.network.retry_pending_messages, 
                        daemon=True).start()
        
        clear_screen()
        print(f"局域网聊天工具已启动")
        print(f"本机IP: {self.my_ip}")
        print(f"监听端口: {PORT}")
        print(f"输入 '帮助' 查看指令\n")
        
        while True:
            try:
                self.main_loop()
            except KeyboardInterrupt:
                print("\n正在退出...")
                self.network.stop()
                break
            except Exception as e:
                print(f"\n错误: {e}")
    
    def main_loop(self):
        """主循环"""
        if self.chat_mode and self.current_chat:
            remark = self.data.get_remark(self.current_chat)
            user_input = input(f"{remark}: ").strip()
        else:
            user_input = input("$ ").strip()
        
        if not user_input:
            return
        
        if self.chat_mode and self.current_chat:
            self.handle_chat_input(user_input)
        else:
            self.handle_command(user_input)
    
    def handle_chat_input(self, user_input):
        """处理聊天模式下的输入"""
        if user_input == '退出':
            self.chat_mode = False
            self.current_chat = None
            print("已退出聊天")
            return
        
        if user_input.startswith('/'):
            # 在聊天模式下输入/进入命令模式
            self.handle_command(user_input[1:])
            return
        
        # 发送消息
        target_ip = self.current_chat
        content = user_input
        time_str = datetime.now().strftime('%H:%M:%S')
        
        # 尝试发送
        success, msg = self.network.send_message(target_ip, content)
        
        if success:
            # 清屏并显示自己的消息
            clear_screen()
            print(format_sent_message(content, time_str))
            # 保存消息
            self.data.add_message(target_ip, self.my_ip, content, read=True)
            print(f"\n{msg}")
        else:
            # 离线消息
            self.data.add_pending(target_ip, content)
            self.data.add_message(target_ip, self.my_ip, content, read=True)
            print(f"\n对方不在线，消息将在对方上线后发送")
            # 仍显示消息
            clear_screen()
            print(format_sent_message(content, time_str))
            print("\n[待发送]")
    
    def handle_command(self, command):
        """处理命令"""
        parts = command.split(maxsplit=1)
        cmd = parts[0].lower()
        args = parts[1] if len(parts) > 1 else ''
        
        if cmd in ['帮助', '?']:
            self.show_help()
        elif cmd in ['退出', 'q']:
            self.quit_app()
        elif cmd in ['列表', 'l']:
            self.show_contacts()
        elif cmd in ['联系', 'c']:
            self.start_chat(args)
        elif cmd in ['添加', 'a']:
            self.add_contact(args)
        elif cmd == '备注':
            self.set_remark(args)
        else:
            print(f"未知命令: {cmd}，输入'帮助'查看可用命令")
    
    def show_help(self):
        """显示帮助信息"""
        help_text = """
╔══════════════════════════════════════════════╗
║           局域网聊天工具 - 使用指南           ║
╠══════════════════════════════════════════════╣
║                                              ║
║  【基本概念】                                ║
║  · 提示符 $ 表示等待输入命令                 ║
║  · 提示符 张三: 表示正在与张三聊天           ║
║  · 聊天中输入 / 可临时执行命令              ║
║                                              ║
║  【常用命令】                                ║
║  帮助 或 ?          显示本帮助               ║
║  列表 或 l          查看所有联系人及未读数    ║
║  添加 IP            添加联系人               ║
║  添加 IP 备注       添加联系人并设置备注     ║
║  备注 联系人 新备注 修改联系人备注           ║
║  联系 联系人 或 c   开始与联系人聊天         ║
║  退出 或 q          退出程序                 ║
║                                              ║
║  【聊天操作】                                ║
║  直接输入文字        发送消息                 ║
║  输入 /命令          执行命令                 ║
║  输入 退出           返回命令模式             ║
║                                              ║
║  【提示】                                    ║
║  · 未读消息会自动显示在联系人列表中          ║
║  · 切换到联系人时会自动显示未读消息          ║
║  · 离线消息会在对方上线后自动发送            ║
║  · 所有聊天记录保存在本地                   ║
║                                              ║
╚══════════════════════════════════════════════╝
        """
        print(help_text)
    
    def quit_app(self):
        """退出应用"""
        print("保存数据...")
        self.data.save_all()
        self.network.stop()
        print("再见！")
        os._exit(0)
    
    def show_contacts(self):
        """显示联系人列表"""
        contacts = self.data.list_contacts()
        if not contacts:
            print("暂无联系人")
            return
        
        print("\n联系人列表:")
        print("-" * 50)
        for ip, remark, unread, pending in contacts:
            status = []
            if unread > 0:
                status.append(f"{unread}条未读")
            if pending > 0:
                status.append(f"{pending}条待发送")
            status_str = f" [{', '.join(status)}]" if status else ""
            print(f"  {remark} ({ip}){status_str}")
        print("-" * 50)
    
    def start_chat(self, args):
        """开始与联系人聊天"""
        if not args:
            print("请指定联系人，如：联系 张三")
            return
        
        # 先尝试作为备注查找
        target_ip = self.data.get_ip_by_remark(args)
        if not target_ip:
            # 尝试作为IP
            if args in self.data.contacts:
                target_ip = args
            else:
                print(f"未找到联系人: {args}")
                return
        
        self.current_chat = target_ip
        self.chat_mode = True
        remark = self.data.get_remark(target_ip)
        
        # 显示未读消息
        unread_msgs = self.data.get_unread_messages(target_ip)
        if unread_msgs:
            print("\n[未读消息]")
            for msg in unread_msgs:
                sender = self.data.get_remark(msg['from'])
                print(f"[{msg['time']}] {sender}: {msg['content']}")
            print("--- 以上为未读消息 ---\n")
        
        print(f"正在与 {remark} 聊天 (输入'退出'返回)")
    
    def add_contact(self, args):
        """添加联系人"""
        if not args:
            print("用法: 添加 IP [备注]")
            return
        
        parts = args.split(maxsplit=1)
        ip = parts[0]
        remark = parts[1] if len(parts) > 1 else None
        
        # 简单的IP格式验证
        parts_ip = ip.split('.')
        if len(parts_ip) != 4:
            print(f"无效的IP地址: {ip}")
            return
        
        self.data.add_contact(ip, remark)
        print(f"已添加联系人: {remark or ip} ({ip})")
    
    def set_remark(self, args):
        """设置联系人备注"""
        parts = args.split(maxsplit=1)
        if len(parts) < 2:
            print("用法: 备注 联系人 新备注")
            return
        
        contact_id = parts[0]
        new_remark = parts[1]
        
        # 查找联系人
        target_ip = self.data.get_ip_by_remark(contact_id)
        if not target_ip and contact_id in self.data.contacts:
            target_ip = contact_id
        
        if not target_ip:
            print(f"未找到联系人: {contact_id}")
            return
        
        self.data.contacts[target_ip] = new_remark
        self.data.save_all()
        print(f"已将 {target_ip} 的备注修改为: {new_remark}")

# ==================== 启动 ====================
if __name__ == '__main__':
    app = ChatApp()
    app.start()
