''' This DHT_peer.py file handles the peer node in the DHT network. It is responsible for the following tasks:
    1. Creation of DHT, setup_dht
'''

# Importing the necessary libraries
import socket # for creating and managing sockets (different for m-port and p-port)
import threading # for creating and managing threads (different for m-port and p-port)
import ast # for converting string to list
import csv # for reading and writing csv files
import math # for mathematical operations

# The DHT_peer class
class DHT_peer:
    # the constructor which initializes the required variables
    def __init__(self, manager_addres):
        self.manager_addres = manager_addres # the address of the manager (server) node
        self.manager_port = 42000 # the port of the manager (server) node
        self.peer_name = "peer1" # the name of the peer
        self.peer_IPv4_address = socket.gethostbyname('localhost')
        self.m_port_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # for communication with the manager (server) node
        self.m_port_socket.bind(('localhost', 42001)) # binding the socket to the localhost and port 42001
        self.p_port_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # for communication with the peer nodes
        self.p_port_socket.bind(('localhost', 42002)) # binding the socket to the localhost and port 42002
        self.id = None # the identifier of the peer in the DHT network
        self.ring_size = None # the size of the ring in the DHT network
        self.peers_DHT = None # the list of peers in the DHT network
        self.right_neighbour = None # the right neighbour of the peer in the DHT network
        self.local_hash_table = {} # the local hash table of the peer
        self.can_populate = False # a flag to check if the peer can populate the local hash table
        # registering the peer with the manager (server) node
        self.register_with_manager()

        # the thread for receiving messages from the manager (server) node
        self.m_port_thread = threading.Thread(target=self.receive_m_port)
        self.m_port_thread.start()

        # the thread for receiving messages from the peer nodes
        self.p_port_thread = threading.Thread(target=self.receive_p_port)
        self.p_port_thread.start()

    # the method that listens for the messages from the manager (server) node
    def receive_m_port(self):
        while True:
            # receiving the message from the manager (server) node
            m_data, m_address = self.m_port_socket.recvfrom(1024)
            # decoding the message
            m_data = m_data.decode('utf-8')
            # print data
            print(m_data)
    
    # the method that listens for the messages from the peer nodes
    def receive_p_port(self):
        while True:
            # receiving the message from the peer nodes
            p_data, p_address = self.p_port_socket.recvfrom(1024)
            # decoding the message
            p_data = p_data.decode('utf-8')
            # print data
            print(p_data)
            #split the message into a list on the basis of space
            p_data = p_data.split(" ")
            # check the command received
            if p_data[0] == "set_id": # if the command is set_id
                set_id_thread = threading.Thread(target=self.set_id, args=(*p_data[1:],)) # create a thread for the set_id method
                set_id_thread.start()
            elif p_data[0] == "store": # if the command is store
                store_dht_thread = threading.Thread(target=self.store_dht, args=(*p_data[1:],)) # create a thread for the store_dht method
                store_dht_thread.start()
            elif p_data[0] == "get-record-count": # if the command is get-record-count
                get_record_count_thread = threading.Thread(target=self.get_record_count, args=())
                get_record_count_thread.start()
            else: # if the command is invalid
                print("Invalid command received from the peer node.")
    
    # the method that registers the peer with the manager (server) node
    def register_with_manager(self):
        # first, send the command to the manager (server) node to register the peer
        # the command is of the form "register <peer_name> <IPv4_address> <m_port> <p_port>"
        register_command = "register " + self.peer_name + " " + self.peer_IPv4_address + " " + str(42001) + " " + str(42002)
        self.m_port_socket.sendto(register_command.encode('utf-8'), (self.manager_addres, self.manager_port)) # sending the command to the manager (server) node

        # wait for the response from the manager (server) node
        # the response is either of the form "FAILURE: <reason>" or "SUCCESS"
        response, manager_address = self.m_port_socket.recvfrom(1024)
        response = response.decode('utf-8') # decoding the response

        # if the response is SUCCESS, then the peer is successfully registered with the manager (server) node
        if response == "SUCCESS":
            print("Peer registered successfully with the manager (server) node.")
        else:
            # print the response to better understand the reason for failure
            print(response)
            # exit the program
            exit()
    
    # the method that sets up the DHT network
    def setup_dht(self):
        # first, send the command to the manager (server) node to setup the DHT network
        # the command is of the form "setup-dht <peer_name> <n> <YYYY>"
        setup_dht_command = "setup-dht " + self.peer_name + " " + str(3) + " " + str(1950)
        self.m_port_socket.sendto(setup_dht_command.encode('utf-8'), (self.manager_addres, self.manager_port)) # sending the command to the manager (server) node

        # wait for the response from the manager (server) node
        # the response is either of the form "FAILURE: <reason>" or "SUCCESS <a string containing the dht_list 3-tuple elements of the form (peer_name, peer_ipv4, p_port)>"
        response, manager_address = self.m_port_socket.recvfrom(1024)
        response = response.decode('utf-8') # decoding the response

        # if the response is SUCCESS, then we have received the "SUCCESS <a string containing the dht_list which is a list of 3-tuple elements of the form (peer_name, peer_ipv4, p_port)>" response
        if response.startswith("SUCCESS"):
            _, dht_list_str = response.split("\n",1) # splitting the response to get the dht_list string
            dht_list = ast.literal_eval(dht_list_str) # converting the string to list
            self.peers_DHT = [(peer_name, peer_ipv4, int(p_port)) for peer_name, peer_ipv4, p_port in dht_list] # converting the list of strings to list of 3-tuple elements
        
        self.id = 0 # the identifier of the peer in the DHT network as it is the leader
        self.ring_size = 3 # the size of the ring in the DHT network
        self.right_neighbour = self.peers_DHT[(self.id+1)%self.ring_size] # setting the right neighbour of the peer in the DHT network
        print("Peer " + self.peer_name + " has been set up with the following details:")
        print("Identifier: " + str(self.id))
        print("Ring size: " + str(self.ring_size))

        # send the set_id command to the right neigbour of the peer in the DHT network
        set_id_command = "set_id " + str(self.id) + " " + str(self.ring_size) + " " + str(self.peers_DHT)
        self.p_port_socket.sendto(set_id_command.encode('utf-8'), (self.right_neighbour[1], self.right_neighbour[2]))

        # wait until all the peers have identifiers and the ring size set
        while not self.can_populate:
            pass

        # populate the local hash table of the peer
        self.populate_dht()

        # print the configuration of the local hash table of the peer
        self.print_configuration()

        # send the command dht_complete to the manager (server) node to indicate that the DHT network has been set up
        # the command is of the form "dht-complete <peer_name>"
        dht_complete_command = "dht-complete " + self.peer_name
        self.m_port_socket.sendto(dht_complete_command.encode('utf-8'), (self.manager_addres, self.manager_port))
    
    # the method that sets the identifier of the peer in the DHT network
    def set_id(self, *args):
        # if the id is already set, then return as the assingment process is complete
        if self.id is not None:
            self.can_populate = True
            return
        #split the arguments into variables
        # the command is received of the form "set_id <id> <ring_size> <dht_list>"
        self.id = int(args[0]) # the identifier of the peer in the DHT network
        self.ring_size = int(args[1]) # the size of the ring in the DHT network
        peer_DHT = ast.literal_eval(args[2]) # converting the string to list
        self.peers_DHT = [(peer_name, peer_ipv4, int(p_port)) for peer_name, peer_ipv4, p_port in peer_DHT] # converting the list of strings to list of 3-tuple elements

        # setting the right neighbour of the peer in the DHT network
        self.right_neighbour = self.peers_DHT[(self.id+1)%self.ring_size]
        print("Peer " + self.peer_name + " has been set up with the following details:")
        print("Identifier: " + str(self.id))
        print("Ring size: " + str(self.ring_size))

        # send the set_id command to the right neigbour of the peer
        set_id_command = "set_id " + str(self.id) + " " + str(self.ring_size) + " " + str(self.peers_DHT)
        self.p_port_socket.sendto(set_id_command.encode('utf-8'), (self.right_neighbour[1], self.right_neighbour[2]))

    # a method for populating the local hash table of the peer
    def populate_dht(self):
        # open the csv file containing the data to be stored in the local hash tables of the peers
        with open (f'details-1950.csv', 'r') as file: # open the csv file in read mode
            reader = csv.reader(file) # create a reader object
            next(reader) # skip the header row
            events = list(reader) # convert the reader object to a list (easy to iterate over)
            s = self.next_prime(2 * len(events)) # find the next prime number 2 times greater than the number of events
            for event in events: # iterate over the events
                event_id = int(event[0]) # the event id of the event
                pos = event_id % s # the position of the event in the local hash table
                id = pos % self.ring_size # the identifier of the peer in the DHT network that is responsible for storing the event
                if id == self.id: # if the current peer is the intended peer for storing the event
                    self.local_hash_table[pos] = event # store the event in the local hash table of the peer
                else:
                    # send the store command to the right neigbour of the peer
                    store_command = "store " + str(pos) + " " + event
                    self.p_port_socket.sendto(store_command.encode('utf-8'), (self.right_neighbour[1], self.right_neighbour[2]))
            
    # a method for the finding the next prime number 2 times greater than n
    def next_prime(self, n):
        while True: # keep iterating until a prime number is found
            n += 1 # increment n by 1
            for i in range(2, int(math.sqrt(n)) + 1): # iterate from 2 to the square root of n
                # if n is divisible by i, then n is not a prime number
                if n % i == 0:
                    break
            else: # if n is not divisible by any number from 2 to the square root of n, then n is a prime number
                return n


    # a method for storing the data in the local hash table of the peer
    def store_dht(self, *args):
        #split the arguments into variables
        # the command is received of the form "store <pos> <event>"
        pos = int(args[0])
        event = args[1]

        # check if the current peer is the intended peer for storing the data
        id = pos % self.ring_size
        if id == self.id: # if the current peer is the intended peer for storing the data
            self.local_hash_table[pos] = event # store the data in the local hash table of the peer
            print("Data stored successfully in the local hash table of the peer " + self.peer_name + ".")
        else:
            # send the store command to the right neigbour of the peer
            store_command = "store " + str(pos) + " " + event
            self.p_port_socket.sendto(store_command.encode('utf-8'), (self.right_neighbour[1], self.right_neighbour[2]))
    
    # a method for getting the record count of the local hash table of the peer
    def get_record_count(self):
        record_count = len(self.local_hash_table)

        # send the record count to the leader of the DHT network
        record_count_command = str(record_count)
        self.p_port_socket.sendto(record_count_command.encode('utf-8'), (self.peers_DHT[0][1], self.peers_DHT[0][2]))

    # a method that prints the number of records stored in each node of the DHT network
    def print_configuration(self):
        for node in range(self.ring_size):
            # send the get-record-count command to each node of the DHT network
            get_record_count_command = "get-record-count"
            self.p_port_socket.sendto(get_record_count_command.encode('utf-8'), (self.peers_DHT[node][1], self.peers_DHT[node][2]))

            #get the record count from the peer
            record_count, _ = self.p_port_socket.recvfrom(1024)
            record_count = record_count.decode('utf-8')
            print("The number of records stored in the local hash table of the peer " + self.peers_DHT[node][0] + " is " + record_count + ".")
        