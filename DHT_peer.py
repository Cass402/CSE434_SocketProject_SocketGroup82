''' This DHT_peer.py file handles the peer node in the DHT network. It is responsible for the following tasks:
    1. Creation of DHT, setup_dht
'''

# Importing the necessary libraries
import socket # for creating and managing sockets (different for m-port and p-port)
import threading # for creating and managing threads (different for m-port and p-port)
import ast # for converting string to list
import csv # for reading and writing csv files
import math # for mathematical operations
import json # for encoding and decoding json data
import random # for generating random numbers

# The DHT_peer class
class DHT_peer:
    # the constructor which initializes the required variables
    def __init__(self, manager_addres, manager_port, peer_name, peer_IPv4_address, m_port, p_port):
        self.manager_addres = manager_addres # the address of the manager (server) node
        self.manager_port = manager_port # the port of the manager (server) node
        self.peer_name = peer_name # the name of the peer
        self.peer_IPv4_address = peer_IPv4_address # the IPv4 address of the peer
        self.m_port = m_port # the port for communication with the manager (server) node
        self.p_port = p_port # the port for communication with the peer nodes
        self.m_port_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # for communication with the manager (server) node
        self.m_port_socket.bind((self.peer_IPv4_address, self.m_port)) # binding the socket to the localhost and port 42001
        self.p_port_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # for communication with the peer nodes
        self.p_port_socket.bind((self.peer_IPv4_address, self.p_port)) # binding the socket to the localhost and port 42002
        self.id = None # the identifier of the peer in the DHT network
        self.ring_size = None # the size of the ring in the DHT network
        self.peers_DHT = None # the list of peers in the DHT network
        self.right_neighbour = None # the right neighbour of the peer in the DHT network
        self.local_hash_table = {} # the local hash table of the peer
        self.printed = False # a flag to check if the configuration of the local hash table has been printed
        self.can_populate = False # a flag to check if the peer can populate the local hash table
        self.event_id_set = (5536849, 2402920, 5539287, 55770111)
        self.listen_p_port = True # a flag to check if the peer should listen for messages from the peer nodes
        self.teardown_complete = False # a flag to check if the teardown process is complete
        self.leaving_or_joining = False # a flag to check if the peer is leaving or joining the DHT network
        # registering the peer with the manager (server) node
        self.register_with_manager()

        '''# the thread for receiving messages from the manager (server) node
        self.m_port_thread = threading.Thread(target=self.receive_m_port)
        self.m_port_thread.start()'''

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
            # check if the peer should listen for messages from the peer nodes
            if not self.listen_p_port:
                continue
            p_data, p_address = self.p_port_socket.recvfrom(1024)
            # decoding the message
            p_data = p_data.decode('utf-8')
            # print data
            print(p_data)
            #split the message into a list on the basis of space
            p_data = p_data.split(" ",1)
            # check the command received
            if p_data[0] == "set_id": # if the command is set_id
                set_id_thread = threading.Thread(target=self.set_id, args=(p_data[1],)) # create a thread for the set_id method
                set_id_thread.start()
            elif p_data[0] == "store": # if the command is store
                store_dht_thread = threading.Thread(target=self.store_dht, args=(p_data[1],)) # create a thread for the store_dht method
                store_dht_thread.start()
            elif p_data[0] == "print_configuration": # if the command is print_configuration
                print_configuration_thread = threading.Thread(target=self.print_configuration) # create a thread for the print_configuration method
                print_configuration_thread.start()
            elif p_data[0] == "find-event": # if the command is find-event
                find_event_thread = threading.Thread(target=self.find_event, args=(p_data[1],))
                find_event_thread.start()
            elif p_data[0] == "teardown": # if the command is teardown
                teardown_thread = threading.Thread(target=self.delete_local_hash_table) # create a thread for the teardown_dht method
                teardown_thread.start()
            elif p_data[0] == "reset-id":
                reset_id_thread = threading.Thread(target=self.reset_id, args=(p_data[1],p_address[0], p_address[1]))
                reset_id_thread.start()
            elif p_data[0] == "join-dht":
                join_rebuild_thread = threading.Thread(target=self.join_rebuild, args=(p_data[1],))
                join_rebuild_thread.start()
            elif p_data[0] == "rebuild-dht":
                if self.leaving_or_joining:
                    # this means that populating is done and the new leader has rebuilt the DHT network
                    # send dht-rebuilt command to the manager (server) node
                    # the command is of the form "dht-rebuilt <peer_name> <name of the new leader>"
                    # find the new leader by checking the peers_DHT and comparing the IP address and port number
                    new_leader = [peer for peer in self.peers_DHT if peer[1] == p_address[0] and peer[2] == p_address[1]]
                    dht_rebuilt_command = "dht-rebuilt " + self.peer_name + " " + new_leader[0][0]
                    self.m_port_socket.sendto(dht_rebuilt_command.encode('utf-8'), (self.manager_addres, self.manager_port))
                    self.leaving_or_joining = False
                    continue
                self.populate_dht()
                # after the populating has been done, send the rebuild-dht command back to the same peer
                rebuild_dht_command = "rebuild-dht"
                self.p_port_socket.sendto(rebuild_dht_command.encode('utf-8'), (p_address[0], p_address[1]))
            else: # if the command is invalid
                print("Invalid command received from the peer node.")
    
    # the method that registers the peer with the manager (server) node
    def register_with_manager(self):
        # first, send the command to the manager (server) node to register the peer
        # the command is of the form "register <peer_name> <IPv4_address> <m_port> <p_port>"
        register_command = "register " + self.peer_name + " " + self.peer_IPv4_address + " " + str(self.m_port) + " " + str(self.p_port)
        self.m_port_socket.sendto(register_command.encode('utf-8'), (self.manager_addres, self.manager_port)) # sending the command to the manager (server) node

        # wait for the response from the manager (server) node
        # the response is either of the form "FAILURE: <reason>" or "SUCCESS"
        response, _ = self.m_port_socket.recvfrom(1024)
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
        setup_dht_command = "setup-dht " + self.peer_name + " " + str(5) + " " + str(1996)
        self.m_port_socket.sendto(setup_dht_command.encode('utf-8'), (self.manager_addres, self.manager_port)) # sending the command to the manager (server) node

        # wait for the response from the manager (server) node
        # the response is either of the form "FAILURE: <reason>" or "SUCCESS <a string containing the dht_list 3-tuple elements of the form (peer_name, peer_ipv4, p_port)>"
        response, manager_address = self.m_port_socket.recvfrom(1024)
        response = response.decode('utf-8') # decoding the response
        print("breakpoint1")

        # if the response is SUCCESS, then we have received the "SUCCESS <a string containing the dht_list which is a list of 3-tuple elements of the form (peer_name, peer_ipv4, p_port)>" response
        if response.startswith("SUCCESS"):
            _, dht_list_str = response.split("\n",1) # splitting the response to get the dht_list string
            dht_list = ast.literal_eval(dht_list_str) # converting the string to list
            self.peers_DHT = [(peer_name, peer_ipv4, int(p_port)) for peer_name, peer_ipv4, p_port in dht_list] # converting the list of strings to list of 3-tuple elements
        
        print("breakpoint2")
        self.id = 0 # the identifier of the peer in the DHT network as it is the leader
        self.ring_size = 5 # the size of the ring in the DHT network
        self.right_neighbour = self.peers_DHT[(self.id+1)%self.ring_size] # setting the right neighbour of the peer in the DHT network
        print("Peer " + self.peer_name + " has been set up with the following details:")
        print("Identifier: " + str(self.id))
        print("Ring size: " + str(self.ring_size))

        # send the set_id command to the right neigbour of the peer in the DHT network
        set_id_command = "set_id " + str(self.id+1) + " " + str(self.ring_size) + " " + json.dumps(self.peers_DHT)
        self.p_port_socket.sendto(set_id_command.encode('utf-8'), (self.right_neighbour[1], self.right_neighbour[2]))

        # wait until all the peers have identifiers and the ring size set
        while not self.can_populate:
            pass

        # populate the local hash table of the peer
        self.populate_dht()

        # print the configuration of the local hash table of the peer
        self.print_configuration()

        # send the dht-complete command to the manager (server) node
        dht_complete_command = "dht-complete " + self.peer_name
        self.m_port_socket.sendto(dht_complete_command.encode('utf-8'), (self.manager_addres, self.manager_port))

        # wait for the response from the manager (server) node
        # the response is either of the form "FAILURE: <reason>" or "SUCCESS"
        response, _ = self.m_port_socket.recvfrom(1024)
        response = response.decode('utf-8') # decoding the response

    
    # the method that sets the identifier of the peer in the DHT network
    def set_id(self, p_data):
        # if the id is already set, then return as the assingment process is complete
        if self.id is not None:
            self.can_populate = True
            return
        #split the p_data into three variables
        p_data = p_data.split(" ",2)
        self.id = int(p_data[0]) # the identifier of the peer in the DHT network
        self.ring_size = int(p_data[1])
        self.peers_DHT = json.loads(p_data[2]) # the list of peers in the DHT network
        self.local_hash_table = {} # the local hash table of the peer

        # setting the right neighbour of the peer in the DHT network
        self.right_neighbour = self.peers_DHT[(self.id+1)%self.ring_size]
        print("Peer " + self.peer_name + " has been set up with the following details:")
        print("Identifier: " + str(self.id))
        print("Ring size: " + str(self.ring_size))

        # send the set_id command to the right neigbour of the peer
        set_id_command = "set_id " + str(self.id+1) + " " + str(self.ring_size) + " " + json.dumps(self.peers_DHT)
        self.p_port_socket.sendto(set_id_command.encode('utf-8'), (self.right_neighbour[1], self.right_neighbour[2]))

    # a method for populating the local hash table of the peer
    def populate_dht(self):
        # open the csv file containing the data to be stored in the local hash tables of the peers
        with open (f'details-1996.csv', 'r') as file: # open the csv file in read mode
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
                    store_command = "store " + str(pos) + " " + json.dumps(event)
                    self.p_port_socket.sendto(store_command.encode('utf-8'), (self.right_neighbour[1], self.right_neighbour[2]))
        self.can_populate = False
            
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
    def store_dht(self, p_data):
        #split the p_data into two variables
        p_data = p_data.split(" ",1)
        pos = int(p_data[0]) # the position of the data in the local hash table
        event = json.loads(p_data[1]) # the data to be stored in the local hash table

        # check if the current peer is the intended peer for storing the data
        id = pos % self.ring_size
        if id == self.id: # if the current peer is the intended peer for storing the data
            self.local_hash_table[pos] = event # store the data in the local hash table of the peer
            print("Data stored successfully in the local hash table of the peer " + self.peer_name + ".")
        else:
            # send the store command to the right neigbour of the peer
            store_command = "store " + str(pos) + " " + json.dumps(event)
            self.p_port_socket.sendto(store_command.encode('utf-8'), (self.right_neighbour[1], self.right_neighbour[2]))

    # a method that prints the number of records stored in each node of the DHT network
    def print_configuration(self):

        # if the configuration has already been printed, then return
        if self.printed:
            return

        self.printed = True
        print("The number of records stored in the local hash table of the peer " + self.peer_name + " is " + str(len(self.local_hash_table)) + ".")

        # send a command to the right neighbour of the peer to print the configuration of the local hash table of the peer
        print_configuration_command = "print_configuration"
        self.p_port_socket.sendto(print_configuration_command.encode('utf-8'), (self.right_neighbour[1], self.right_neighbour[2]))

    # a method that queries the DHT for a specific event_id record
    def query_dht(self):
        # first, send the command to the manager (server) node to query the DHT network
        # the command is of the form "query-dht <peer_name>" with peer_name being the name of the peer sending the query
        query_dht_command = "query-dht " + self.peer_name
        self.m_port_socket.sendto(query_dht_command.encode('utf-8'), (self.manager_addres, self.manager_port))

        # wait for the response from the manager (server) node
        # the response is either of the form "FAILURE: <reason>" or "SUCCESS <a string containing the 3-tuple element (peer_name, peer_ipv4, p_port) which is a random peer in the DHT network>"
        response, _ = self.m_port_socket.recvfrom(1024)
        response = response.decode('utf-8')

        # if the response is SUCCESS, then we have received the "SUCCESS <a string containing the 3-tuple element (peer_name, peer_ipv4, p_port) which is a random peer in the DHT network>" response
        if response.startswith("SUCCESS"):
            _, peer_in_DHT = response.split("\n",1) # splitting the response to get the peer_in_DHT string
            peer_in_DHT = ast.literal_eval(peer_in_DHT)
            peer_in_DHT = (peer_in_DHT[0], peer_in_DHT[1], int(peer_in_DHT[2]))
        
        # send the find-event command to the peer_in_DHT
        # the command is of the form "find-event <event_id> <a string containing the 3-tuple element (peer_name, peer_ipv4, p_port) of the peer sending the query> id-seq"
        self.listen_p_port = False
        find_event_command = "find-event " + str(self.event_id_set[0]) + " " + json.dumps((self.peer_name, self.peer_IPv4_address, self.p_port)) + " id-seq"
        self.p_port_socket.sendto(find_event_command.encode('utf-8'), (peer_in_DHT[1], peer_in_DHT[2]))

        # wait for the response
        # the response is either of the form "FAILURE" or "SUCCESS <a string containing the event record> <id_seq>"
        response, _ = self.p_port_socket.recvfrom(1024)
        response = response.decode('utf-8')

        # if the response is FAILURE, then the query failed
        if response == "FAILURE":
            print("Storm event " + str(self.event_id_set[0]) + " not found in the DHT.")
        else:
            _, event_record_id_seq = response.split("\n",1) # splitting the response to get the event record and id_seq
            event_record, id_seq = event_record_id_seq.split(" ",1) # splitting the event record and id_seq
            event_record = json.loads(event_record) # converting the event record to a dictionary
            print("Storm event " + str(self.event_id_set[0]) + " found in the DHT.")
            print("The event record is: " + str(event_record))
            print("The id_seq is: " + id_seq)

        self.listen_p_port = True # continue listening for messages from the peer nodes            

    def find_event(self, p_data):
        # split the p_data into three variables
        p_data = p_data.split(" ",2)
        event_id = int(p_data[0])
        peer_sending_query = json.loads(p_data[1])
        peer_sending_query = (peer_sending_query[0], peer_sending_query[1], int(peer_sending_query[2]))
        id_seq = p_data[2]
        I = [x for x in range(0, self.ring_size)] # the list of identifiers of the peers in the DHT network
        if id_seq == "id-seq":
            id_seq = ""
        else:
            visited = [int(x) for x in id_seq.split(",").strip()] # the list of identifiers of the peers that have been visited to find the event_id
            I = [x for x in I if x not in visited] # remove the visited identifiers from the list of identifiers to still be visted

        # compute the pos and id
        s = self.next_prime(2 * len(self.local_hash_table))
        pos = event_id % s
        id = pos % self.ring_size

        # check if the id is the same as the current peer
        if id == self.id:
            # check if the event_id is in the local hash table
            if pos in self.local_hash_table:
                # send the response to the peer_sending_query
                id_seq += str(self.id)
                response = "SUCCESS\n" + json.dumps(self.local_hash_table[pos]) + " " + id_seq
                self.p_port_socket.sendto(response.encode('utf-8'), (peer_sending_query[1], peer_sending_query[2]))
                return
            else: # if the event_id is not in the local hash table
                # send the response to the peer_sending_query
                self.p_port_socket.sendto("FAILURE".encode('utf-8'), (peer_sending_query[1], peer_sending_query[2]))
                return
        else: # if the id is not the same as the current peer
            # update the I to remove the id of the current peer as it has been visited
            I = [x for x in I if x != self.id]
            # if I is empty, then query failed
            if len(I) == 0:
                # send the response to the peer_sending_query
                self.p_port_socket.sendto("FAILURE".encode('utf-8'), (peer_sending_query[1], peer_sending_query[2]))
                return
            # update id_seq to include the id of the current peer as it has been visited
            id_seq += str(self.id) + ","
            # choose a random id from I and send the find-event command to the peer with that id
            next = random.choice(I)
            # find the peer with the next id
            next_peer = self.peers_DHT[next]
            # send the find-event command to the next_peer
            find_event_command = "find-event " + str(event_id) + " " + str(peer_sending_query) + " " + id_seq
            self.p_port_socket.sendto(find_event_command.encode('utf-8'), (next_peer[1], next_peer[2]))
    
    # the method the initiates the leave-dht process for the peer
    def leave_dht(self):
        # first, send the command to the manager (server) node to leave the DHT network
        # the command is of the form "leave-dht <peer_name>"
        leave_dht_command = "leave-dht " + self.peer_name
        self.m_port_socket.sendto(leave_dht_command.encode('utf-8'), (self.manager_addres, self.manager_port))

        # wait for the response from the manager (server) node
        # the response is either of the form "FAILURE: <reason>" or "SUCCESS: Left the DHT"
        response, _ = self.m_port_socket.recvfrom(1024)
        response = response.decode('utf-8')

        # if the response is failure, then print the reason for failure
        if response.startswith("FAILURE"):
            print(response)
            return
        
        self.leaving_or_joining = True
        # if the response is failure, then the peer is ready to leave the DHT network
        # initiate the teardown process for leaving the DHT network (different from the normal teardown process)
        self.teardown_dht()

    # the method that initiates the join-dht process for the peer
    def join_dht(self):
        # first, send the command to the manager (server) node to join the DHT network
        # the command is of the form "join-dht <peer_name>"
        join_dht_command = "join-dht " + self.peer_name
        self.m_port_socket.sendto(join_dht_command.encode('utf-8'), (self.manager_addres, self.manager_port))

        # wait for the response from the manager (server) node
        # the response is either of the form "FAILURE: <reason>" or "SUCCESS\n<the 3-tuple element (peer_name, peer_ipv4, p_port) of the leader of the DHT network>"
        response, _ = self.m_port_socket.recvfrom(1024)
        response = response.decode('utf-8')

        # if the response is failure, then print the reason for failure
        if response.startswith("FAILURE"):
            print(response)
            return
        
        # if the response is success, then the peer is ready to join the DHT network
        # send the join-dht command to the leader of the DHT network along with the details of the peer
        _, leader = response.split("\n",1)
        leader = ast.literal_eval(leader)
        leader = (leader[0], leader[1], int(leader[2]))
        join_dht_command = "join-dht " + str((self.peer_name, self.peer_IPv4_address, self.p_port))
        self.p_port_socket.sendto(join_dht_command.encode('utf-8'), (leader[1], leader[2]))
    
    # the method that tears down the DHT network
    def teardown_dht(self):
        # first, check if the peer initiating the teardown is the leader
        if self.id == 0:
            # start the normal teardown process as it is not linked to leave-dht or join-dht
            self.normal_teardown()
        else:
            # if the id is not 0, then it is the case that a peer initiated the leave-dht process
            # first, initiate a teardown by sending the teardown command to the right neighbour of the peer in the DHT network
            self.teardown_complete = True
            teardown_command = "teardown"
            self.p_port_socket.sendto(teardown_command.encode('utf-8'), (self.right_neighbour[1], self.right_neighbour[2]))

            #initiate the renumbering process of the peers in the DHT network by sending reset-id command to the right neighbour of the peer
            # the command is of the form "reset-id <id which the right neighbour of the peer should use> <ring_size to be used by the right neighbour of the peer> <the id of leaving peer so as to remove it from the list of peers in the DHT network>"
            reset_id_command = "reset-id " + str(0) + " " + str(self.ring_size - 1) + " " + str(self.id)
            self.p_port_socket.sendto(reset_id_command.encode('utf-8'), (self.right_neighbour[1], self.right_neighbour[2])) 
    
    # the method that initiates the normal teardown process
    def normal_teardown(self):
        # first, send the command to the manager (server) node to teardown the DHT network
        # the command is of the form "teardown-dht <peer_name>"
        teardown_dht_command = "teardown-dht " + self.peer_name
        self.m_port_socket.sendto(teardown_dht_command.encode('utf-8'), (self.manager_addres, self.manager_port))

        # wait for the response from the manager (server) node
        # the response is either of the form "FAILURE: <reason>" or "SUCCESS"
        response, _ = self.m_port_socket.recvfrom(1024)
        response = response.decode('utf-8')

        # if the response is SUCCESS, then the DHT network is ready to be torn down
        # check if the response is a FAILURE, then print the reason for failure
        if response.startswith("FAILURE"):
            print(response)
            return
        
        # delete the local hash table of the peer and set the teardown_complete flag to True
        self.local_hash_table = {}
        self.teardown_complete = True
        
        # send the teardown command to the right neighbour of the peer in the DHT network
        teardown_command = "teardown"
        self.p_port_socket.sendto(teardown_command.encode('utf-8'), (self.right_neighbour[1], self.right_neighbour[2]))
    
    # the method that deletes the local hash table of the peer
    def delete_local_hash_table(self):
        # delete the local hash table of the peer
        self.local_hash_table = {}
        
        # check if the all the peers have completed the teardown process by checking the if the teardown_complete flag is True
        if self.teardown_complete and self.id == 0 and not self.leaving_or_joining:
            # send the teardown-complete command to the manager (server) node
            teardown_complete_command = "teardown-complete " + self.peer_name
            self.m_port_socket.sendto(teardown_complete_command.encode('utf-8'), (self.manager_addres, self.manager_port))
        elif self.teardown_complete and self.id == 0 and self.leaving_or_joining:
            return
        elif self.teardown_complete and self.id != 0:
            # this case happens when the teardown initiated is due to the leave-dht process
            return
        else:
            # send the teardown command to the right neighbour of the peer in the DHT network
            teardown_command = "teardown"
            self.p_port_socket.sendto(teardown_command.encode('utf-8'), (self.right_neighbour[1], self.right_neighbour[2]))
    
    # the method that resets the identifier of the peer in the DHT network
    def reset_id(self, p_data, p_address, p_port):
        #split the p_data into three variables (id, ring_size, leaving_peer_id)
        p_data = p_data.split(" ",2)
        id = int(p_data[0])
        ring_size = int(p_data[1])
        leaving_peer_id = int(p_data[2])

        # check if the leaving_or_joining flag is True, that means the reset-id process is finished
        if self.leaving_or_joining:
            # send the rebuild_dht command to the right neighbour of the peer (the new leader)
            rebuild_dht_command = "rebuild-dht"
            self.p_port_socket.sendto(rebuild_dht_command.encode('utf-8'), (self.right_neighbour[1], self.right_neighbour[2]))
            return
        
        # update the id of the current peer
        self.id = id
        self.ring_size = ring_size

        # remove the leaving_peer from the list of peers in the DHT network by checking the peer IP address and port number
        self.peers_DHT = [peer for peer in self.peers_DHT if peer[1] != p_address and peer[2] != p_port]

        # send the reset-id command to the right neighbour of the peer
        reset_id_command = "reset-id " + str(id+1) + " " + str(ring_size) + " " + str(leaving_peer_id)
        self.p_port_socket.sendto(reset_id_command.encode('utf-8'), (self.right_neighbour[1], self.right_neighbour[2]))

        # change the right neighbour of the peer
        self.right_neighbour = self.peers_DHT[(self.id+1)%self.ring_size]

    # the method that rebuilds the DHT network for the joining peer
    def join_rebuild(self, p_data):
        #the p_data contains the details of the joining peer
        p_data = ast.literal_eval(p_data)
        joining_peer = (p_data[0], p_data[1], int(p_data[2]))

        # add the joining_peer to the list of peers in the DHT network
        self.peers_DHT.append(joining_peer)

        self.id = 0
        self.ring_size += 1
        # find the new right neighbour of the peer
        self.right_neighbour = self.peers_DHT[(self.id+1)%self.ring_size]
        self.local_hash_table = {} # the local hash table of the peer

        # send the set_id command to the new right neighbour of the peer
        set_id_command = "set_id " + str(self.id+1) + " " + str(self.ring_size) + " " + json.dumps(self.peers_DHT)
        self.p_port_socket.sendto(set_id_command.encode('utf-8'), (self.right_neighbour[1], self.right_neighbour[2]))

        # while the peer is not ready to populate the local hash table, keep waiting
        while not self.can_populate:
            pass

        # populate the local hash table of the peer
        self.populate_dht()

        # send the rebuild-dht command to the joining peer as confirmation of the completion of the rebuilding and joining process
        rebuild_dht_command = "rebuild-dht"
        self.p_port_socket.sendto(rebuild_dht_command.encode('utf-8'), (joining_peer[1], joining_peer[2]))

        return
   
# the main method
if __name__ == "__main__":
    # ask the user to enter the manager address, manager_port, peer_name, peer_IPv4_address, m_port, p_port
    manager_addres = input("Enter the address of the manager (server) node: ")
    manager_port = int(input("Enter the port of the manager (server) node: "))
    peer_name = input("Enter the name of the peer: ")
    peer_IPv4_address = input("Enter the IPv4 address of the peer: ")
    m_port = int(input("Enter the port for communication with the manager (server) node: "))
    p_port = int(input("Enter the port for communication with the peer nodes: "))
    # create the DHT_peer object
    peer = DHT_peer(manager_addres, manager_port, peer_name, peer_IPv4_address, m_port, p_port)
    #peer.setup_dht() # setup the DHT network
    #ask the user if they want to leave the DHT network
    leave = input("Do you want to leave the DHT network? (yes/no): ")
    if leave == "yes":
        peer.leave_dht() # leave the DHT network
    #ask the user if they want to query the DHT network
    query = input("Do you want to query the DHT network? (yes/no): ")
    if query == "yes":
        peer.query_dht() # query the DHT network
    #ask the user if they want to join the DHT network
    join = input("Do you want to join the DHT network? (yes/no): ")
    if join == "yes":
        peer.join_dht() # join the DHT network
    '''#ask the user if they want to teardown the DHT network
    teardown = input("Do you want to teardown the DHT network? (yes/no): ")
    if teardown == "yes":
        peer.teardown_dht() # teardown the DHT network'''
    #ask the user if they want to deregiseter the peer with the manager (server) node
    deregister = input("Do you want to deregister the peer with the manager (server) node? (yes/no): ")
    if deregister == "yes":
        peer.deregister_with_manager() # deregister the peer with the manager (server) node