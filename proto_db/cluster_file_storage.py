from __future__ import annotations

import io
import json
import logging
import os
import socket
import threading
import time
import uuid
from typing import Dict, List, Optional, Tuple, Any

from . import common
from .common import BlockProvider, AtomPointer
from .exceptions import ProtoUnexpectedException
from .fsm import FSM
from .standalone_file_storage import StandaloneFileStorage

_logger = logging.getLogger(__name__)

# Default network settings
DEFAULT_PORT = 8765
DEFAULT_VOTE_TIMEOUT_MS = 5000  # 5 seconds
DEFAULT_READ_TIMEOUT_MS = 2000  # 2 seconds
DEFAULT_RETRY_INTERVAL_MS = 1000  # 1 second
DEFAULT_MAX_RETRIES = 3

# Message types for server communication
MSG_TYPE_VOTE_REQUEST = "vote_request"
MSG_TYPE_VOTE_RESPONSE = "vote_response"
MSG_TYPE_PAGE_REQUEST = "page_request"
MSG_TYPE_PAGE_RESPONSE = "page_response"
MSG_TYPE_ROOT_UPDATE = "root_update"
MSG_TYPE_HEARTBEAT = "heartbeat"


class ClusterNetworkManager:
    """
    Manages network communication between servers in the cluster.

    This class handles sending and receiving messages between servers,
    including vote requests/responses, page requests/responses, and root updates.
    """

    def __init__(self,
                 server_id: str,
                 host: str,
                 port: int,
                 servers: List[Tuple[str, int]],
                 vote_timeout_ms: int = DEFAULT_VOTE_TIMEOUT_MS,
                 read_timeout_ms: int = DEFAULT_READ_TIMEOUT_MS,
                 retry_interval_ms: int = DEFAULT_RETRY_INTERVAL_MS,
                 max_retries: int = DEFAULT_MAX_RETRIES):
        """
        Initialize the network manager.

        Args:
            server_id: Unique identifier for this server
            host: Host address to bind to
            port: Port to listen on
            servers: List of (host, port) tuples for all servers in the cluster
            vote_timeout_ms: Timeout for vote requests in milliseconds
            read_timeout_ms: Timeout for page requests in milliseconds
            retry_interval_ms: Interval between retries in milliseconds
            max_retries: Maximum number of retry attempts
        """
        self.server_id = server_id
        self.host = host
        self.port = port
        self.servers = servers
        self.vote_timeout_ms = vote_timeout_ms
        self.read_timeout_ms = read_timeout_ms
        self.retry_interval_ms = retry_interval_ms
        self.max_retries = max_retries

        self.socket = None
        self.running = False
        self.listen_thread = None
        self.storage = None  # Reference to the ClusterFileStorage instance

        # Callbacks for handling different message types
        self.message_handlers = {
            MSG_TYPE_VOTE_REQUEST: self._handle_vote_request,
            MSG_TYPE_VOTE_RESPONSE: self._handle_vote_response,
            MSG_TYPE_PAGE_REQUEST: self._handle_page_request,
            MSG_TYPE_PAGE_RESPONSE: self._handle_page_response,
            MSG_TYPE_ROOT_UPDATE: self._handle_root_update,
            MSG_TYPE_HEARTBEAT: self._handle_heartbeat
        }

        # State for vote requests
        self.vote_requests = {}
        self.vote_responses = {}
        self.vote_lock = threading.Lock()

        # State for page requests
        self.page_requests = {}
        self.page_responses = {}
        self.page_lock = threading.Lock()

        # Cleanup thread for old requests
        self.cleanup_thread = None
        self.cleanup_interval_ms = 60000  # 1 minute

        # FSM for managing server state
        self.fsm = self._create_fsm()

        _logger.info(f"Initialized ClusterNetworkManager for server {server_id} at {host}:{port}")

    def _create_fsm(self) -> FSM:
        """
        Create the FSM for managing server state.

        Returns:
            FSM: The created FSM instance
        """
        fsm_definition = {
            'Initializing': {
                'Initializing': self._on_initializing,
                'Start': self._on_start
            },
            'Running': {
                'VoteRequest': self._on_vote_request,
                'VoteResponse': self._on_vote_response,
                'PageRequest': self._on_page_request,
                'PageResponse': self._on_page_response,
                'RootUpdate': self._on_root_update,
                'Heartbeat': self._on_heartbeat,
                'Stop': self._on_stop
            },
            'Stopping': {
                'Stopped': self._on_stopped
            }
        }

        return FSM(fsm_definition)

    def start(self):
        """
        Start the network manager.

        This method starts the socket server and begins listening for incoming messages.
        It also starts a cleanup thread for old requests.
        """
        if self.running:
            return

        self.running = True
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((self.host, self.port))
        self.socket.settimeout(0.1)  # Short timeout to allow checking self.running

        self.listen_thread = threading.Thread(target=self._listen_for_messages)
        self.listen_thread.daemon = True
        self.listen_thread.start()

        # Start cleanup thread
        self.cleanup_thread = threading.Thread(target=self._cleanup_old_requests)
        self.cleanup_thread.daemon = True
        self.cleanup_thread.start()

        self.fsm.send_event({'name': 'Start'})
        _logger.info(f"Started ClusterNetworkManager for server {self.server_id}")

    def stop(self):
        """
        Stop the network manager.

        This method stops the socket server and joins the listener and cleanup threads.
        """
        if not self.running:
            return

        self.running = False
        self.fsm.send_event({'name': 'Stop'})

        if self.listen_thread:
            self.listen_thread.join(timeout=2.0)

        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=2.0)

        if self.socket:
            self.socket.close()
            self.socket = None

        _logger.info(f"Stopped ClusterNetworkManager for server {self.server_id}")

    def _listen_for_messages(self):
        """
        Listen for incoming messages from other servers.

        This method runs in a separate thread and processes incoming messages.
        """
        while self.running:
            try:
                data, addr = self.socket.recvfrom(65536)  # Max UDP packet size
                message = json.loads(data.decode('utf-8'))

                msg_type = message.get('type')
                if msg_type in self.message_handlers:
                    self.message_handlers[msg_type](message, addr)
                else:
                    _logger.warning(f"Received unknown message type: {msg_type}")

            except socket.timeout:
                # This is expected due to the socket timeout
                continue
            except json.JSONDecodeError:
                _logger.error(f"Received invalid JSON data")
            except Exception as e:
                _logger.exception(f"Error processing message: {e}")

    def _send_message(self, host: str, port: int, message: Dict[str, Any]) -> bool:
        """
        Send a message to a specific server.

        Args:
            host: Host address to send to
            port: Port to send to
            message: Message to send

        Returns:
            bool: True if the message was sent successfully, False otherwise
        """
        try:
            data = json.dumps(message).encode('utf-8')
            self.socket.sendto(data, (host, port))
            return True
        except Exception as e:
            _logger.error(f"Error sending message to {host}:{port}: {e}")
            return False

    def _broadcast_message(self, message: Dict[str, Any]) -> int:
        """
        Broadcast a message to all servers in the cluster.

        Args:
            message: Message to broadcast

        Returns:
            int: Number of servers the message was successfully sent to
        """
        success_count = 0
        for host, port in self.servers:
            if self._send_message(host, port, message):
                success_count += 1
        return success_count

    # FSM event handlers

    def _on_initializing(self, event):
        """Handle the Initializing event."""
        _logger.debug(f"Initializing ClusterNetworkManager for server {self.server_id}")

    def _on_start(self, event):
        """Handle the Start event."""
        self.fsm.change_state('Running')
        _logger.debug(f"ClusterNetworkManager for server {self.server_id} is now running")

    def _on_vote_request(self, event):
        """Handle a vote request event."""
        request_id = event.get('request_id')
        requester_id = event.get('requester_id')
        _logger.debug(f"Processing vote request {request_id} from {requester_id}")

    def _on_vote_response(self, event):
        """Handle a vote response event."""
        request_id = event.get('request_id')
        responder_id = event.get('responder_id')
        vote_granted = event.get('vote_granted', False)
        _logger.debug(f"Received vote response for request {request_id} from {responder_id}: {vote_granted}")

    def _on_page_request(self, event):
        """Handle a page request event."""
        request_id = event.get('request_id')
        requester_id = event.get('requester_id')
        wal_id = event.get('wal_id')
        offset = event.get('offset')
        _logger.debug(f"Processing page request {request_id} from {requester_id} for WAL {wal_id} at offset {offset}")

    def _on_page_response(self, event):
        """Handle a page response event."""
        request_id = event.get('request_id')
        responder_id = event.get('responder_id')
        _logger.debug(f"Received page response for request {request_id} from {responder_id}")

    def _on_root_update(self, event):
        """Handle a root update event."""
        root_pointer = event.get('root_pointer')
        updater_id = event.get('updater_id')
        _logger.debug(f"Received root update from {updater_id}: {root_pointer}")

    def _on_heartbeat(self, event):
        """Handle a heartbeat event."""
        sender_id = event.get('sender_id')
        _logger.debug(f"Received heartbeat from {sender_id}")

    def _on_stop(self, event):
        """Handle the Stop event."""
        self.fsm.change_state('Stopping')
        _logger.debug(f"Stopping ClusterNetworkManager for server {self.server_id}")

    def _on_stopped(self, event):
        """Handle the Stopped event."""
        _logger.debug(f"ClusterNetworkManager for server {self.server_id} has stopped")

    # Message handlers

    def _handle_vote_request(self, message, addr):
        """
        Handle a vote request message from another server.

        Args:
            message: The vote request message
            addr: The address of the sender
        """
        request_id = message.get('request_id')
        requester_id = message.get('requester_id')

        # Process the vote request and decide whether to grant the vote
        # Check if we've already voted for another server recently
        vote_granted = True
        current_time = time.time()

        with self.vote_lock:
            # Check if we've already voted for another server in the last vote_timeout_ms
            for req_id, req_time in list(self.vote_requests.items()):
                if current_time - req_time < (self.vote_timeout_ms / 1000):
                    # We've already voted for another server recently
                    vote_granted = False
                    _logger.debug(f"Denying vote to {requester_id} because we've already voted recently")
                    break

            # If we're granting the vote, record this request
            if vote_granted:
                self.vote_requests[request_id] = current_time

        # Send the vote response
        response = {
            'type': MSG_TYPE_VOTE_RESPONSE,
            'request_id': request_id,
            'responder_id': self.server_id,
            'vote_granted': vote_granted
        }

        self._send_message(addr[0], addr[1], response)
        _logger.debug(f"Sent vote response to {requester_id}: {vote_granted}")

        self.fsm.send_event({'name': 'VoteRequest', 'request_id': request_id, 'requester_id': requester_id})

    def _handle_vote_response(self, message, addr):
        """
        Handle a vote response message from another server.

        Args:
            message: The vote response message
            addr: The address of the sender
        """
        request_id = message.get('request_id')
        responder_id = message.get('responder_id')
        vote_granted = message.get('vote_granted', False)

        with self.vote_lock:
            if request_id in self.vote_requests:
                if request_id not in self.vote_responses:
                    self.vote_responses[request_id] = {}

                self.vote_responses[request_id][responder_id] = vote_granted

        self.fsm.send_event({
            'name': 'VoteResponse',
            'request_id': request_id,
            'responder_id': responder_id,
            'vote_granted': vote_granted
        })

    def _handle_page_request(self, message, addr):
        """
        Handle a page request message from another server.

        Args:
            message: The page request message
            addr: The address of the sender
        """
        request_id = message.get('request_id')
        requester_id = message.get('requester_id')
        wal_id_str = message.get('wal_id')
        offset = message.get('offset')
        size = message.get('size', 4096)  # Default to 4KB if not specified

        try:
            # Convert string WAL ID to UUID
            wal_id = uuid.UUID(wal_id_str)

            # Get the data from the storage
            data = None

            # Check if we have a reference to the ClusterFileStorage
            if hasattr(self, 'storage') and self.storage:
                # First check in-memory segments
                with self.storage._lock:
                    if (wal_id, offset) in self.storage.in_memory_segments:
                        data = self.storage.in_memory_segments[(wal_id, offset)]

                # If not found in memory, try to read from disk
                if not data:
                    try:
                        reader = self.storage.block_provider.get_reader(wal_id, offset)
                        with reader as stream:
                            data = stream.read(size)
                    except Exception as e:
                        _logger.debug(f"Failed to read from disk for WAL {wal_id} at offset {offset}: {e}")

            # Encode the data as base64 for transmission
            import base64
            encoded_data = base64.b64encode(data).decode('utf-8') if data else None

            # Send the response
            response = {
                'type': MSG_TYPE_PAGE_RESPONSE,
                'request_id': request_id,
                'responder_id': self.server_id,
                'wal_id': wal_id_str,
                'offset': offset,
                'data': encoded_data
            }

            self._send_message(addr[0], addr[1], response)
            _logger.debug(f"Sent page response for request {request_id} to {addr[0]}:{addr[1]}")

        except Exception as e:
            _logger.error(f"Error handling page request: {e}")
            # Send an error response
            response = {
                'type': MSG_TYPE_PAGE_RESPONSE,
                'request_id': request_id,
                'responder_id': self.server_id,
                'wal_id': wal_id_str,
                'offset': offset,
                'error': str(e),
                'data': None
            }
            self._send_message(addr[0], addr[1], response)

        self.fsm.send_event({
            'name': 'PageRequest',
            'request_id': request_id,
            'requester_id': requester_id,
            'wal_id': wal_id_str,
            'offset': offset
        })

    def _handle_page_response(self, message, addr):
        """
        Handle a page response message from another server.

        Args:
            message: The page response message
            addr: The address of the sender
        """
        request_id = message.get('request_id')
        responder_id = message.get('responder_id')
        wal_id = message.get('wal_id')
        offset = message.get('offset')
        encoded_data = message.get('data')
        error = message.get('error')

        # Decode the data if it exists
        data = None
        if encoded_data:
            try:
                import base64
                data = base64.b64decode(encoded_data)
            except Exception as e:
                _logger.error(f"Error decoding page data: {e}")
                error = f"Error decoding data: {e}"

        with self.page_lock:
            if request_id in self.page_requests:
                self.page_responses[request_id] = {
                    'responder_id': responder_id,
                    'wal_id': wal_id,
                    'offset': offset,
                    'data': data,
                    'error': error
                }

        self.fsm.send_event({
            'name': 'PageResponse',
            'request_id': request_id,
            'responder_id': responder_id,
            'wal_id': wal_id,
            'offset': offset,
            'error': error
        })

    def _handle_root_update(self, message, addr):
        """
        Handle a root update message from another server.

        Args:
            message: The root update message
            addr: The address of the sender
        """
        updater_id = message.get('updater_id')
        transaction_id_str = message.get('transaction_id')
        offset = message.get('offset')

        try:
            # Convert string transaction ID to UUID
            transaction_id = uuid.UUID(transaction_id_str)

            # Update the root object if we have a reference to the storage
            if hasattr(self, 'storage') and self.storage:
                # Create an AtomPointer for the new root
                root_pointer = AtomPointer(transaction_id, offset)

                # Update the root object locally without broadcasting
                # (to avoid infinite loops of updates)
                self.storage.block_provider.update_root_object(root_pointer)
                _logger.info(f"Updated root object from server {updater_id}: {transaction_id_str}:{offset}")
        except Exception as e:
            _logger.error(f"Error handling root update: {e}")

        self.fsm.send_event({
            'name': 'RootUpdate',
            'updater_id': updater_id,
            'transaction_id': transaction_id_str,
            'offset': offset
        })

    def _handle_heartbeat(self, message, addr):
        """
        Handle a heartbeat message from another server.

        Args:
            message: The heartbeat message
            addr: The address of the sender
        """
        sender_id = message.get('sender_id')
        self.fsm.send_event({'name': 'Heartbeat', 'sender_id': sender_id})

    # Public API

    def request_vote(self) -> Tuple[bool, int]:
        """
        Request votes from other servers to obtain exclusive lock.

        Returns:
            Tuple[bool, int]: (success, votes_received)
                success: True if majority of votes were received, False otherwise
                votes_received: Number of positive votes received
        """
        request_id = str(uuid.uuid4())

        with self.vote_lock:
            self.vote_requests[request_id] = time.time()
            self.vote_responses[request_id] = {}

        # Broadcast vote request to all servers
        message = {
            'type': MSG_TYPE_VOTE_REQUEST,
            'request_id': request_id,
            'requester_id': self.server_id
        }

        self._broadcast_message(message)
        _logger.debug(f"Sent vote request {request_id} to all servers")

        # Wait for responses with timeout
        start_time = time.time()
        majority = len(self.servers) // 2 + 1

        while time.time() - start_time < (self.vote_timeout_ms / 1000):
            with self.vote_lock:
                responses = self.vote_responses.get(request_id, {})
                positive_votes = sum(1 for vote in responses.values() if vote)

                if positive_votes >= majority:
                    _logger.info(f"Received majority of votes: {positive_votes}/{len(self.servers)}")
                    return True, positive_votes

            time.sleep(0.1)

        # Timeout reached
        with self.vote_lock:
            responses = self.vote_responses.get(request_id, {})
            positive_votes = sum(1 for vote in responses.values() if vote)

            # Clean up this request
            if request_id in self.vote_requests:
                del self.vote_requests[request_id]
            if request_id in self.vote_responses:
                del self.vote_responses[request_id]

        _logger.warning(f"Vote request timed out. Received {positive_votes}/{len(self.servers)} votes")
        return positive_votes >= majority, positive_votes

    def request_page(self, wal_id: uuid.UUID, offset: int, size: int = 4096) -> Optional[bytes]:
        """
        Request a page from other servers.

        Args:
            wal_id: WAL ID
            offset: Offset in the WAL
            size: Size of the page to request

        Returns:
            Optional[bytes]: The requested page data, or None if not found
        """
        request_id = str(uuid.uuid4())

        with self.page_lock:
            self.page_requests[request_id] = {
                'wal_id': str(wal_id),
                'offset': offset,
                'size': size,
                'time': time.time()
            }

        # Broadcast page request to all servers
        message = {
            'type': MSG_TYPE_PAGE_REQUEST,
            'request_id': request_id,
            'requester_id': self.server_id,
            'wal_id': str(wal_id),
            'offset': offset,
            'size': size
        }

        self._broadcast_message(message)

        # Wait for responses with timeout
        start_time = time.time()

        while time.time() - start_time < (self.read_timeout_ms / 1000):
            with self.page_lock:
                if request_id in self.page_responses:
                    response = self.page_responses[request_id]
                    if response.get('data'):
                        # Clean up the request and response
                        del self.page_requests[request_id]
                        del self.page_responses[request_id]
                        return response['data']
                    elif response.get('error'):
                        _logger.warning(f"Error in page response: {response['error']}")

            time.sleep(0.1)

        # Timeout reached, no valid response
        with self.page_lock:
            if request_id in self.page_requests:
                del self.page_requests[request_id]
            if request_id in self.page_responses:
                del self.page_responses[request_id]

        return None

    def broadcast_root_update(self, transaction_id: uuid.UUID, offset: int) -> int:
        """
        Broadcast a root update to all servers.

        Args:
            transaction_id: Transaction ID of the new root
            offset: Offset of the new root

        Returns:
            int: Number of servers the update was successfully sent to
        """
        message = {
            'type': MSG_TYPE_ROOT_UPDATE,
            'updater_id': self.server_id,
            'transaction_id': str(transaction_id),
            'offset': offset
        }

        return self._broadcast_message(message)

    def send_heartbeat(self) -> int:
        """
        Send a heartbeat to all servers.

        Returns:
            int: Number of servers the heartbeat was successfully sent to
        """
        message = {
            'type': MSG_TYPE_HEARTBEAT,
            'sender_id': self.server_id
        }

        return self._broadcast_message(message)

    def _cleanup_old_requests(self):
        """
        Periodically clean up old vote and page requests.

        This method runs in a separate thread and removes old requests and responses
        to prevent memory leaks.
        """
        while self.running:
            try:
                current_time = time.time()

                # Clean up old vote requests
                with self.vote_lock:
                    for request_id in list(self.vote_requests.keys()):
                        request_time = self.vote_requests[request_id]
                        if current_time - request_time > (self.vote_timeout_ms / 1000) * 2:
                            del self.vote_requests[request_id]
                            if request_id in self.vote_responses:
                                del self.vote_responses[request_id]

                # Clean up old page requests
                with self.page_lock:
                    for request_id in list(self.page_requests.keys()):
                        request_time = self.page_requests[request_id]['time']
                        if current_time - request_time > (self.read_timeout_ms / 1000) * 2:
                            del self.page_requests[request_id]
                            if request_id in self.page_responses:
                                del self.page_responses[request_id]

                # Sleep for the cleanup interval
                time.sleep(self.cleanup_interval_ms / 1000)

            except Exception as e:
                _logger.error(f"Error in cleanup thread: {e}")
                time.sleep(1)  # Sleep for a short time to avoid tight loop on error


class ClusterFileStorage(StandaloneFileStorage):
    """
    An implementation of cluster file storage with support for distributed operations.

    This class extends StandaloneFileStorage to add support for distributed operations
    in a cluster environment, including vote-based exclusive locking, root synchronization,
    and cached page retrieval between servers.
    """

    def __init__(self,
                 block_provider: BlockProvider,
                 server_id: str = None,
                 host: str = None,
                 port: int = None,
                 servers: List[Tuple[str, int]] = None,
                 vote_timeout_ms: int = None,
                 read_timeout_ms: int = None,
                 retry_interval_ms: int = None,
                 max_retries: int = None,
                 buffer_size: int = common.MB,
                 blob_max_size: int = common.GB * 2,
                 max_workers: int = (os.cpu_count() or 1) * 5):
        """
        Constructor for the ClusterFileStorage class.

        Args:
            block_provider: The underlying storage provider
            server_id: Unique identifier for this server (defaults to a UUID)
            host: Host address to bind to
            port: Port to listen on
            servers: List of (host, port) tuples for all servers in the cluster
            vote_timeout_ms: Timeout for vote requests in milliseconds
            read_timeout_ms: Timeout for page requests in milliseconds
            retry_interval_ms: Interval between retries in milliseconds
            max_retries: Maximum number of retry attempts
            buffer_size: Size of the WAL buffer in bytes
            blob_max_size: Maximum size of a blob in bytes
            max_workers: Number of worker threads for asynchronous operations
        """
        super().__init__(
            block_provider=block_provider,
            buffer_size=buffer_size,
            blob_max_size=blob_max_size,
            max_workers=max_workers
        )

        # Load configuration from block provider if available
        config = block_provider.get_config_data()

        # Initialize cluster-specific attributes with defaults from config or constants
        self.server_id = server_id or config.get('cluster', 'server_id', fallback=str(uuid.uuid4()))
        self.host = host or config.get('cluster', 'host', fallback="localhost")
        self.port = port or config.getint('cluster', 'port', fallback=DEFAULT_PORT)

        # Parse servers from config if not provided
        if servers is None:
            self.servers = []
            if config.has_section('servers'):
                for server_key in config['servers']:
                    if server_key.startswith('server_'):
                        server_value = config['servers'][server_key]
                        host, port = server_value.split(':')
                        self.servers.append((host, int(port)))
            if not self.servers:
                _logger.warning("No servers configured, cluster functionality will be limited")
        else:
            self.servers = servers

        # Load timeouts and retry settings
        self.vote_timeout_ms = vote_timeout_ms or config.getint('cluster', 'vote_timeout_ms',
                                                                fallback=DEFAULT_VOTE_TIMEOUT_MS)
        self.read_timeout_ms = read_timeout_ms or config.getint('cluster', 'read_timeout_ms',
                                                                fallback=DEFAULT_READ_TIMEOUT_MS)
        self.retry_interval_ms = retry_interval_ms or config.getint('cluster', 'retry_interval_ms',
                                                                    fallback=DEFAULT_RETRY_INTERVAL_MS)
        self.max_retries = max_retries or config.getint('cluster', 'max_retries', fallback=DEFAULT_MAX_RETRIES)

        _logger.info(f"Cluster configuration: server_id={self.server_id}, host={self.host}, port={self.port}, "
                     f"servers={self.servers}, vote_timeout_ms={self.vote_timeout_ms}, "
                     f"read_timeout_ms={self.read_timeout_ms}, retry_interval_ms={self.retry_interval_ms}, "
                     f"max_retries={self.max_retries}")

        # Initialize network manager
        self.network_manager = ClusterNetworkManager(
            server_id=self.server_id,
            host=self.host,
            port=self.port,
            servers=self.servers,
            vote_timeout_ms=self.vote_timeout_ms,
            read_timeout_ms=self.read_timeout_ms,
            retry_interval_ms=self.retry_interval_ms,
            max_retries=self.max_retries
        )

        # Set the storage reference in the network manager
        self.network_manager.storage = self

        # Start the network manager
        self.network_manager.start()

        # Additional locks for cluster operations
        self.root_lock = threading.Lock()

        _logger.info(f"Initialized ClusterFileStorage for server {self.server_id}")

    def read_current_root(self) -> AtomPointer:
        """
        Read the current root object.

        Returns:
            AtomPointer: The current root object pointer
        """
        return self.block_provider.get_current_root_object()

    def read_lock_current_root(self) -> AtomPointer:
        """
        Read and lock the current root object.

        This method acquires a distributed lock on the root object before reading it.

        Returns:
            AtomPointer: The current root object pointer
        """
        # Acquire distributed lock through voting
        success, votes = self.network_manager.request_vote()

        if not success:
            _logger.warning(f"Failed to acquire distributed lock for root update (received {votes} votes)")
            raise ProtoUnexpectedException(message=f"Failed to acquire distributed lock for root update")

        # Read the root object
        with self.root_lock:
            return self.read_current_root()

    def set_current_root(self, root_pointer: AtomPointer):
        """
        Set the current root object.

        This method updates the root object and broadcasts the update to all servers.

        Args:
            root_pointer: The new root object pointer
        """
        # Update the root object locally
        self.block_provider.update_root_object(root_pointer)

        # Broadcast the update to all servers
        servers_updated = self.network_manager.broadcast_root_update(
            root_pointer.transaction_id,
            root_pointer.offset
        )

        _logger.info(f"Updated root object and notified {servers_updated} servers")

    def unlock_current_root(self):
        """
        Unlock the current root object.

        This method releases the distributed lock on the root object.
        """
        # In this implementation, the lock is automatically released when the vote timeout expires
        # No explicit action needed
        pass

    def get_reader(self, wal_id: uuid.UUID, position: int) -> io.BytesIO:
        """
        Get a reader for the specified WAL at the given position.

        This method first tries to get the data from the local cache, then from the
        local file system, and finally from other servers in the cluster.

        Args:
            wal_id: WAL ID
            position: Position in the WAL

        Returns:
            io.BytesIO: A reader for the WAL data
        """
        # First check if the data is in the in-memory cache
        with self._lock:
            if (wal_id, position) in self.in_memory_segments:
                return io.BytesIO(self.in_memory_segments[(wal_id, position)])

        try:
            # Try to get the data from the local file system
            return self.block_provider.get_reader(wal_id, position)
        except Exception as e:
            _logger.debug(f"Failed to read from local file system: {e}")

            # Try to get the data from other servers
            for retry in range(self.max_retries):
                data = self.network_manager.request_page(wal_id, position)
                if data:
                    return io.BytesIO(data)

                if retry < self.max_retries - 1:
                    time.sleep(self.retry_interval_ms / 1000)

            # If all retries fail, raise an exception
            raise ProtoUnexpectedException(
                message=f"Failed to read WAL {wal_id} at position {position} from any server"
            )

    def close(self):
        """
        Close the storage, flushing any pending writes and releasing resources.

        This method extends the base implementation to also stop the network manager.
        """
        # Stop the network manager
        if hasattr(self, 'network_manager') and self.network_manager:
            self.network_manager.stop()

        # Call the parent implementation to handle the rest
        super().close()

        _logger.info(f"Closed ClusterFileStorage for server {self.server_id}")
