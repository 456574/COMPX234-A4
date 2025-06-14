import socket
import sys
import time

class TupleSpaceClient:
    def __init__(self, host, port, request_file):
        self.host = host
        self.port = port
        self.request_file = request_file
        # Create TCP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    def connect(self):
        """Connect to the tuple space server"""
        try:
            self.sock.connect((self.host, self.port))
            print(f"Connected to {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"Connection error: {e}")
            return False
    
    def process_requests(self):
        """Process requests from the input file"""
        try:
            with open(self.request_file, 'r') as file:
                # Process each line in the file
                for line_number, line in enumerate(file, 1):
                    line = line.strip()
                    if not line:
                        continue  # Skip empty lines
                    
                    # Validate request format
                    if self.validate_request(line, line_number):
                        # Send request and get response
                        response = self.send_request(line)
                        # Print request and response
                        print(f"{line}: {response}")
        except FileNotFoundError:
            print(f"Error: File '{self.request_file}' not found")
        except Exception as e:
            print(f"Processing error: {e}")
        finally:
            self.sock.close()
            print("Connection closed")
    
    def validate_request(self, request, line_number):
        """Validate request format and constraints"""
        parts = request.split(' ', 2)
        if not parts:
            print(f"Line {line_number}: Empty request")
            return False
        
        op = parts[0]
        # Validate operation type
        if op not in ['PUT', 'GET', 'READ']:
            print(f"Line {line_number}: Invalid operation '{op}'")
            return False
        
        # Validate PUT operation format
        if op == 'PUT' and len(parts) < 3:
            print(f"Line {line_number}: PUT requires key and value")
            return False
        
        # Validate minimum requirements
        if len(parts) < 2:
            print(f"Line {line_number}: Missing key")
            return False
        
        key = parts[1]
        value = parts[2] if len(parts) > 2 else None
        
        # Check collated size constraint
        collated_size = len(key) + (len(value) if value else 0)
        if collated_size > 970:
            print(f"Line {line_number}: Collated size {collated_size} > 970 chars")
            return False
        
        return True
    
    def send_request(self, request):
        """Send request to server and receive response"""
        try:
            # Parse request components
            parts = request.split(' ', 2)
            
            # Map operation to protocol code
            if parts[0] == 'PUT':
                op_char = 'P'
                key, value = parts[1], parts[2]
                msg = f"{op_char} {key} {value}"
            elif parts[0] == 'GET':
                op_char = 'G'
                key = parts[1]
                msg = f"{op_char} {key}"
            else:  # READ
                op_char = 'R'
                key = parts[1]
                msg = f"{op_char} {key}"
            
            # Format message with length prefix
            msg_length = len(msg) + 4  # 3 for length + 1 space
            formatted = f"{msg_length:03d} {msg}"
            
            # Send request to server
            self.sock.sendall(formatted.encode())
            
            # Receive response header (3 bytes)
            header = self.sock.recv(3)
            if not header:
                return "ERR no response"
            
            # Parse response length
            msg_length = int(header)
            # Receive full response
            response = self.sock.recv(msg_length - 3).decode()
            # Remove length prefix from response
            return response[4:]
            
        except Exception as e:
            return f"ERR client error: {e}"
if __name__ == "__main__":
    # Validate command line arguments
    if len(sys.argv) != 4:
        print("Usage: python client.py <host> <port> <request_file>")
        sys.exit(1)
    
    host = sys.argv[1]
    port = int(sys.argv[2])
    request_file = sys.argv[3]
    
    # Create and run client
    client = TupleSpaceClient(host, port, request_file)
    if client.connect():
        client.process_requests()