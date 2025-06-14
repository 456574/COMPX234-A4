class TupleSpaceServer:
    def __init__(self, port):
        self.port = port
        # Dictionary to store key-value pairs
        self.tuple_space = {}
        
        # Statistics tracking
        self.stats = {
            'start_time': datetime.now(),
            'total_connections': 0,
            'total_operations': 0,
            'reads': 0,
            'gets': 0,
            'puts': 0,
            'errors': 0,
            'current_connections': 0,
            'tuple_count': 0,
            'tuple_sizes': deque(maxlen=1000),  # Track sizes for averaging
            'key_sizes': deque(maxlen=1000),
            'value_sizes': deque(maxlen=1000)
        }
        
        # Thread synchronization lock
        self.lock = threading.Lock()
        self.running = True
        
        # Create TCP socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
    def start(self):
        """Start the server and listen for incoming connections"""
        try:
            # Bind to all interfaces and specified port
            self.server_socket.bind(('0.0.0.0', self.port))
            self.server_socket.listen(5)
            print(f"Server started on port {self.port}")
            
            # Start statistics reporting thread
            stats_thread = threading.Thread(target=self.report_stats, daemon=True)
            stats_thread.start()
            
            # Main server loop
            while self.running:
                # Accept new client connection
                client_sock, addr = self.server_socket.accept()
                with self.lock:
                    self.stats['total_connections'] += 1
                    self.stats['current_connections'] += 1
                
                # Create new thread to handle client
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_sock, addr)
                )
                client_thread.start()
        except Exception as e:
            print(f"Server error: {e}")
        finally:
            self.server_socket.close()