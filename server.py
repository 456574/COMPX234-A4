import socket
import threading
import time
import json
from collections import deque
from datetime import datetime
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
    def handle_client(self, client_sock, addr):
        """Handle communication with a connected client"""
        print(f"New connection from {addr}")
        try:
            while True:
                # Read 3-byte message length header
                header = client_sock.recv(3)
                if not header:
                    break  # Client disconnected
                
                # Parse message length
                msg_length = int(header)
                # Read the actual message
                data = client_sock.recv(msg_length).decode()
                if not data:
                    break
                
                # Process request and get response
                response = self.process_request(data)
                # Send response back to client
                client_sock.send(response.encode())
                
        except ConnectionResetError:
            print(f"Client {addr} disconnected abruptly")
        except Exception as e:
            print(f"Error with client {addr}: {e}")
        finally:
            # Update connection stats
            with self.lock:
                self.stats['current_connections'] -= 1
            client_sock.close()
            print(f"Connection closed for {addr}")
    
    def process_request(self, data):
        """Process client request and generate response"""
        try:
            # Split request into components
            parts = data.split(' ', 2)
            if len(parts) < 2:
                return self.format_response("ERR invalid request")
            
            op = parts[0]  # Operation type
            key = parts[1]  # Key
            value = parts[2] if len(parts) > 2 else None  # Value (for PUT)
            
            with self.lock:
                self.stats['total_operations'] += 1
                
                # Route to appropriate handler
                if op == 'R':
                    return self.process_read(key)
                elif op == 'G':
                    return self.process_get(key)
                elif op == 'P':
                    return self.process_put(key, value)
                else:
                    return self.format_response("ERR invalid operation")
        except Exception as e:
            print(f"Request processing error: {e}")
            return self.format_response("ERR server error")       
    def process_read(self, key):
        """Handle READ operation"""
        self.stats['reads'] += 1
        if key in self.tuple_space:
            value = self.tuple_space[key]
            self.record_tuple_stats(key, value)
            return self.format_response(f"OK ({key}, {value}) read")
        return self.format_response(f"ERR {key} does not exist")
    
    def process_get(self, key):
        """Handle GET operation (read and remove)"""
        self.stats['gets'] += 1
        if key in self.tuple_space:
            value = self.tuple_space.pop(key)
            self.stats['tuple_count'] -= 1
            self.record_tuple_stats(key, value)
            return self.format_response(f"OK ({key}, {value}) removed")
        return self.format_response(f"ERR {key} does not exist")
    
    def process_put(self, key, value):
        """Handle PUT operation (add new tuple)"""
        self.stats['puts'] += 1
        # Check if key already exists
        if key in self.tuple_space:
            self.stats['errors'] += 1
            return self.format_response(f"ERR {key} already exists")
        
        # Validate tuple
        if value is None or len(key) > 999 or len(value) > 999:
            self.stats['errors'] += 1
            return self.format_response("ERR invalid tuple")
        
        # Add new tuple
        self.tuple_space[key] = value
        self.stats['tuple_count'] += 1
        self.record_tuple_stats(key, value)
        return self.format_response(f"OK ({key}, {value}) added")
    
    def record_tuple_stats(self, key, value):
        """Record statistics about tuple sizes"""
        self.stats['tuple_sizes'].append(len(key) + len(value))
        self.stats['key_sizes'].append(len(key))
        self.stats['value_sizes'].append(len(value))
    
    def format_response(self, message):
        """Format response with length prefix"""
        # Calculate total message length (including prefix)
        msg_length = len(message) + 3  # 3 bytes for length header
        return f"{msg_length:03d} {message}"
    
    def report_stats(self):
        """Periodically report server statistics"""
        while self.running:
            time.sleep(10)  # Report every 10 seconds
            with self.lock:
                # Calculate averages
                avg_tuple = sum(self.stats['tuple_sizes']) / len(self.stats['tuple_sizes']) if self.stats['tuple_sizes'] else 0
                avg_key = sum(self.stats['key_sizes']) / len(self.stats['key_sizes']) if self.stats['key_sizes'] else 0
                avg_value = sum(self.stats['value_sizes']) / len(self.stats['value_sizes']) if self.stats['value_sizes'] else 0
                
                uptime = datetime.now() - self.stats['start_time']
                
                # Print statistics report
                print("\n" + "="*50)
                print("Server Statistics:")
                print(f"Uptime: {uptime}")
                print(f"Current tuples: {self.stats['tuple_count']}")
                print(f"Avg tuple size: {avg_tuple:.2f} chars")
                print(f"Avg key size: {avg_key:.2f} chars")
                print(f"Avg value size: {avg_value:.2f} chars")
                print(f"Total connections: {self.stats['total_connections']}")
                print(f"Current connections: {self.stats['current_connections']}")
                print(f"Total operations: {self.stats['total_operations']}")
                print(f"READs: {self.stats['reads']}, GETs: {self.stats['gets']}, PUTs: {self.stats['puts']}")
                print(f"Errors: {self.stats['errors']}")
                print("="*50 + "\n")
if __name__ == "__main__":
    import sys
    # Validate command line arguments
    if len(sys.argv) != 2:
        print("Usage: python server.py <port>")
        sys.exit(1)
    
    try:
        port = int(sys.argv[1])
        # Validate port range (50000-59999)
        if not (50000 <= port <= 59999):
            raise ValueError
    except ValueError:
        print("Port must be between 50000 and 59999")
        sys.exit(1)
    
    # Create and start server
    server = TupleSpaceServer(port)
    server.start()