"""multicast class for capturing data with observer/publisher model"""
from threading import Thread, Lock, Event
import socket 
import platform
import time 
from queue import Queue, Full
from typing import Callable, Union
from ipaddress import ip_network, ip_address
from contextlib import suppress

class UDPComm(Thread):

    def __init__(self, address: str, rport: int, sport: int, network_adapter: str):
        """UDP class for sending and receiving data multicast/direct"""

        super().__init__()

        self.t = time.time()
        self.address = address
        self.rport = rport
        self.sport = sport
        self.network_adapter = network_adapter

        self.stop_event = Event()
        self.queue: Queue[bytes] = Queue(maxsize=100)
        self.observers: list[Callable[[bytes], None]] = []

        self.send_lock = Lock()
        self.sock: Union[socket.socket, None] = None

    def connect(self):
        """setup multicast or direct udp socket"""

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(1)

        # check if we are using a multicast address
        if ip_address(self.address) in ip_network("224.0.0.0/4"):

            # switch-case like
            {
                "Linux":   lambda: sock.bind((self.address,         self.rport)),
                "Windows": lambda: sock.bind((self.network_adapter, self.rport))
            }.get(platform.system(), lambda: SystemError("unsupported system"))()
                
            # set multicast output interface, join multicast group
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(self.network_adapter))
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton(self.address) + socket.inet_aton(self.network_adapter))

        else:

            # udp binds directly to interface
            sock.bind((self.network_adapter, port))

        return sock

    def run(self):
        """start reciever and connect to socket"""

        self.sock = self.connect() 

        # signal thread, de-couple multicast receival from observer processing
        _signal_thread = Thread(target=self.signal_thread, daemon=True)
        _signal_thread.start()

        while not self.stop_event.is_set():

            try:
                data, server = self.sock.recvfrom(2 ** 20)
                self.queue.put(data, block=True, timeout=.1)
            except socket.timeout:
                continue
            except Full:
                print("multicast queue full dropping data: observer might be hanging signal thread")

        self.sock.close()

    def signal_thread(self):
        """thread calling observer functions"""
        while not self.stop_event.is_set():
            data = self.queue.get()
            self.signal(data)

    def send_data(self, message: bytes):
        """aquire send lock and prevent sending too fast"""

        with self.send_lock:
            d = time.time() - self.t
            if d < .1: time.sleep(d)
            self.t = time.time()
            self.sock.sendto(message, (self.address, self.sport))

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def start(self):
        self.connect()
        super().start()

    def stop(self):
        self.stop_event.set()
        self.join()

    def signal(self, data):
        """signal all observer functions, print crashed observers"""
        for i, obs in enumerate(self.observers):
            try:
                obs(data)
            except Exception as e:
                print(f"Observer Error: Index:{i} Name:{obs.__name__} Error:{e}")
    
    def loop_forever(self):
        self.stop_event.wait()

if __name__ == "__main__":

    from argparse import ArgumentParser
    from contextlib import suppress

    parser = ArgumentParser(prog="MulticastCapture")
    parser.add_argument("-m", "--mode", type=int, default=0)
    parser.add_argument("-a", "--address", type=str)
    parser.add_argument("-p", "--port", type=int)
    parser.add_argument("-i", "--interface", default="0.0.0.0", type=str)
    parser.add_argument("-d", "--data", default="HelloFromTheOtherSide", type=str)
    args = parser.parse_args()

    def receiver(address, rport, interface):
        """create udpcomm, add observer, wait for messages"""

        with UDPComm(address, rport, rport, interface) as udpComm:

            udpComm.observers.append(lambda d: print(d))

            with suppress(KeyboardInterrupt):
                print("ctrl+c to exit")
                udpComm.loop_forever()

    def sender(address, sport, interface, data):
        """create udpcomm and send data"""

        with UDPComm(address, rport, rport, interface) as udpComm: 
            udpComm.send_data(data)

    print(f"Multicast {'Listening' if args.mode==0 else 'Sending'}")
    print(f"mode: {args.mode}")
    print(f"address: {args.address}")
    print(f"port: {args.port}")
    print(f"interface: {args.interface}")

    if args.mode == 0:
        receiver(args.address, args.port, args.interface)
    else:
        sender(args.address, args.port, args.interface, args.data.encode())
