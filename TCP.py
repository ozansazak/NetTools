import sys
import os
import threading
import socket
import time
import logging

PS_STAT_INIT = -2
PS_STAT_SET = -1
PS_STAT_ACTIVE = 1

SCONF_FILE_DEST = "file"
SCONF_FILE_SEP = "sep"
SCONF_CONF_SERVQ = "pxserv-queue"

SOPTS_SERVID = 0
SOPTS_SERVQUEUE = 1

P_LEVEL_CRITIC = 4
P_LEVEL_ATTENTION = 2
P_LEVEL_ERROR = 3
P_LEVEL_WARN = 1
P_LEVEL_DEBUG = -1

DEF_SERV_CONNS = 8
DEF_BUFSIZ = 1024

err_lvl_desc_tab = {P_LEVEL_CRITIC	: ("[FATAL ERROR]", True),
					P_LEVEL_DEBUG	: ("[DEBUG]", False),
					P_LEVEL_WARN 	: ("[WARNING]", False),
					P_LEVEL_ERROR 	: ("[ERROR]", True),
					P_LEVEL_ATTENTION : ("[!]", False)}

def _panic(err, errorLevel, termination = None):
	# Error level description sign
	err_dsc_sgn = err_lvl_desc_tab.get(errorLevel, -1)
	if err_dsc_sgn == -1:
		raise RuntimeError("Unknown usage of function '_panic' : _panic('{0}', {1}, {2})".format(err, errorLevel, termination))

	# In some of critical error situations, the program must be terminated
	print(err_dsc_sgn[0], err)

	# Termination statements.
	if termination == False:
		return
	elif termination == None:
		if err_dsc_sgn[1] == True:
			sys.exit(1)
	elif termination == True:
		sys.exit(1)
	else:
		_panic("Wrong usage of function '_panic' : _panic('{0}', {1}, {2})".format(err, errorLevel, termination))

def logHandler(logger_name):
	# create logger
	logger = logging.getLogger(logger_name)
	logger.setLevel(logging.DEBUG)

	# create console handler and set level to debug
	ch = logging.StreamHandler()
	ch.setLevel(logging.DEBUG)

	# create formatter
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

	# add formatter to ch
	ch.setFormatter(formatter)

	# add ch to logger
	logger.addHandler(ch)

	return logger

class ProxyServer:
	"Main class for constructing proxy servers using TCP protocol."

	def __init__(self, serverID, servconf):
		self.logger = logHandler(str(serverID))
		self.logger.info("Logger started")

		# Proxy server configurations
		self._config = ProxyServer.pxserv_init_sconf(servconf)
		self._servOpts = {SOPTS_SERVID : serverID}
		self.__servStat = PS_STAT_INIT

		# Proxy server's pool for active sub-servers
		self._psServPool = {}
		self.__activeServCount = 0

		self.logger.info("Server status set to PS_STAT_INIT")

	@classmethod
	def pxserv_init_sconf(cls, config_opts):
		cls.c_file = config_opts[SCONF_FILE_DEST]
		cls.c_sep = config_opts[SCONF_FILE_SEP]

		# Read the configuration file and parse lines
		cls.c_data = [x.split(cls.c_sep) for x in open(cls.c_file).read().splitlines()]
		# Empty return value (dict.)
		cls.r_conf = {}
		for line in cls.c_data:
				cls.r_conf[line[0]] = line[1]

		return cls.r_conf

	def Setup(self):
		self.logger.debug("In-function 'Setup'")

		try:
			self._pxserv_init_setup()
		except Exception as e:
			self.logger.error(e)
			_panic(e, P_LEVEL_ERROR)

		self.__servStat = PS_STAT_SET
		self.logger.info("Server status set to PS_STAT_SET")

	def _pxserv_init_setup(self):
		self.logger.debug("In-function '_pxserv_init_setup'")

		# Check if server is able to set up
		if self.__servStat != PS_STAT_INIT:
			raise RuntimeError("The server is already set.")

		# Make cleanup
		self._psServPool = {}
		self.__activeServCount = 0

		self._servOpts[SOPTS_SERVQUEUE] = int(self._config[SCONF_CONF_SERVQ])

	def AddServer(self, servID, servAddr, servPort):
		self.logger.debug("In-function 'AddServer'")

		# Check for server situation
		if not self._isset():
			self.logger.debug("Return-with-warn from function 'AddServer'")
			return

		if self.__activeServCount != self._servOpts[SOPTS_SERVQUEUE]:
			self.tcp_server = TCPServerHandler(servAddr, servPort)
			self._psServPool[servID] = (servAddr, servPort, self.tcp_server)
			self.tcp_server.start()
			self.logger.info("Constructed new TCP server in {0}:{1}".format(servAddr, servPort))
			self.__activeServCount += 1
		else:
			self.logger.warning("The server has already reached the max. connections")
			_panic("The server has already reached the max. connections", P_LEVEL_WARN)
			self.logger.debug("Return-with-warn from function 'AddServer'")
			return
	def RemoveServer(self, servID):
		self.logger.debug("In-function 'RemoveServer'")

		if not servID in self._psServPool.keys():
			self.logger.warning("There is no such server with ID: " + servID)
			_panic("There is no such server with ID: " + servID, P_LEVEL_WARN)
			self.logger.debug("Return-with-warn from function 'RemoveServer'")
			return

		try:
			self._psServPool[servID][2].stop()
			del self._psServPool[servID]
		except Exception as e:
			self.logger.error(e)
			_panic(e, P_LEVEL_ERROR)
		else:
			self.logger.info("Removed server with ID: " + servID)
			self.__activeServCount -= 1

	def ListServers(self):
		self.logger.debug("In-function 'ListServers'")

		# Check for sever situation
		if not self._isset():
			self.logger.debug("Return-with-warn from function 'ListServers'")
			return

		if len(self._psServPool) == 0:
			_panic("Server pool is empty", P_LEVEL_ATTENTION)
		else:
			print("%10s%10s%10s" % ("ServerID","IP","Port"))
			print("="*30)
			for itm in self._psServPool.items():
				print("%10s%10s%10d" % (itm[0], itm[1][0], itm[1][1]))
			print("")

	def _isset(self):
		# Check for server situation
		if self.__servStat != PS_STAT_SET:
			self.logger.warning("The server must be in state of set.")
			_panic("The server must be in state of set.", P_LEVEL_WARN)
			return False
		else:
			return True

class TCPServerHandler(threading.Thread):
	def __init__(self, servAddr, servPort):
		self._address = (servAddr, servPort)
		self.__servSock = None

		self._activeConnections = {}
		self._activeConnectionsCount = 0
		
		threading.Thread.__init__(self)

	def run(self):
		try:
			self._setup()
			self._activate(DEF_SERV_CONNS)
		except Exception as e:
			_panic("Unexpected error in class 'TCPServerHandler' ({0}:{1}), ".format(self._address[0], self._address[1]) + e, P_LEVEL_CRITIC, termination = False)
			_panic("Now terminating active thread of 'TCPServerHandler' ({0}:{1})".format(self._address[0], self._address[1]), P_LEVEL_DEBUG)
			return

	def _setup(self):
		logging.basicConfig(filename="{0}@{1}.log".format(self._address[0], self._address[1]), level=logging.INFO)
		logging.info("Logging service is started.")

		self.__servSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.__servSock.bind(self._address)
		logging.info("The server has bound to {0}:{1}.".format(self._address[0], self._address[1]))

	def _activate(self, queue):
		logging.info("Activating the server.")
		self.__servSock.listen(queue)
		
		while True:
			self.conn, self.address = self.__servSock.accept()
			if self._activeConnectionsCount != queue:
				logging.info("Got a new connection request from {0}:{1}.".format(self.address[0], self.address[1]))
				self.conn_hnd = TCPClientConnHandler(self.conn, self.address, self._address)
				self.conn_hnd.start()
				self._activeConnections[self.conn] = (self.address, self.conn_hnd)
				self._activeConnectionsCount += 1

	def stop(self):
		_panic("Terminating active thread 'TCPServerHandler' ({0}:{1}), ".format(self._address[0], self._address[1]), P_LEVEL_ATTENTION)
		
		# Terminate active connections
		if self._activeConnectionsCount > 0:
			for itm in self._activeConnections:
				itm[1][1].terminate()
		# Stop TCPServerHandler
		self._Thread__stop()

class TCPClientConnHandler(threading.Thread):
	def __init__(self, connection, address, caller):
		self.__conn = connection
		self._addr = address
		self._callerAddr = caller

		#logging.basicConfig(filename="{0}@{1} <-> {2}@{3}.log".format(self._callerAddr[0], self._callerAddr[1], self._addr[0], self._addr[1]), level=logging.INFO)
		#logging.info("Logging is started")
		self.logger = logging.getLogger(self._addr[0]+":"+str(self._addr[1]))

		threading.Thread.__init__(self)
		
	def run(self):
		try:
			while True:
				self.c_data, self.c_dlen = TCPClientConnHandler.recv_data(self.__conn, DEF_BUFSIZ)
				if self.c_dlen == 0:
					raise RuntimeError("Client not responding. Closing connection.")

				self.logger.info("[<==] {} bytes of data received".format(self.c_dlen))
				#self.logger.info("<INCOMING-DATA>\n"+self.c_data.decode())
				# Find data receiver
				self.d_receiver = TCPClientConnHandler.parse_receiver(self.c_data)
				if self.d_receiver == -1:
					raise RuntimeError("Can not handle client request. Closing.")
				
				if len(self.d_receiver) == 1:
					self.recvr_port = 80
				else:
					self.recvr_port = int(self.d_receiver[1])

				# Construct a new socket for the communication between me and receiver server
				self.__recvrCommSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				# Connect to the receiver
		
				self.__recvrCommSock.connect((self.d_receiver[0], self.recvr_port))
				self.logger.info("Connected to {0}:{1}".format(self.d_receiver[0], self.recvr_port))
				# Send the incoming request to the server
				self.__recvrCommSock.send(self.c_data)
				self.logger.info("[<==] {0} bytes of data sent to {1}:{2}".format(self.c_dlen, self.d_receiver[0], self.recvr_port))

				# Get response
				self.r_data, self.r_dlen = TCPClientConnHandler.recv_data(self.__recvrCommSock, DEF_BUFSIZ)
				if self.r_dlen == 0:
					raise RuntimeError("Server not responding. Closing connection.")

				self.logger.info("[==>] {} bytes of data received".format(self.r_dlen))
				#self.logger.info("<INCOMING-DATA>\n"+self.r_data.decode())

				# Control the response
				self.response_code = TCPClientConnHandler.parse_response_status(self.r_data)
				if self.response_code > 0:
					self.logger.info("Server response : " + self.response_code)
				
				# Send the incoming data to client server
				self.__conn.send(self.r_data)
				self.logger.info("[==>] {} bytes of data sent to client".format(self.r_dlen))

		except Exception as e:
			self.logger.error(e)
			_panic(e, P_LEVEL_CRITIC, termination = False)
			self.terminate()
			return

	@classmethod
	def recv_data(cls, cli_sock, buffer_size):
		cls.buf = b""
		cls.ret = b""
		cls.retlen = 0

		while True:
			cls.buf = cli_sock.recv(buffer_size)

			cls.ret += cls.buf
			cls.retlen += len(cls.buf)

			if len(cls.buf) < buffer_size:
				break

		return (cls.ret, cls.retlen)

	@classmethod
	def parse_receiver(cls, data):
		for line in data.decode().splitlines():
			if line.startswith("Host"):
				cls.addr = line.split(" ")[1] # E.g ["Host:", "127.0.0.1:8080"]
				return cls.addr.split(":")

		return -1

	def terminate(self):
		self.logger.info("Terminating thread.")
		self.__conn.close()
		self._Thread__stop()

	@classmethod
	def parse_response_status(cls, response_data):
		try:
			cls.data = response_data.decode("utf-8")
		except:
			return 0

		for line in cls.data:
			if line.startswith("HTTP"): # E.g "HTTP/1.1 301 Moved Permamently"
				return line.split(" ")[1] 
		# Nothing found
		return -1

if __name__ == "__main__":
	serv = ProxyServer("PXServer-1", {"file" : "server.conf", "sep" : ":"})
	serv.Setup()
	serv.AddServer(10, "localhost", int(sys.argv[1]))
	serv.ListServers()