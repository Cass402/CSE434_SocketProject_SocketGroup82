''' This python file acts as the DHT manager for the CSE 434 Socket Project. 
    It is responsible for handling the DHT and communication between the peers.

    The port numbers available to use are 42000-42499. The DHT manager will use port 42000.

    The requirements for the full project are as follows:
    For the full project deadline, you are to implement the all commands to the manager listed in §1.1. 
    This also involves implementation of all commands issued between peers that are associated with these commands as described in §1.2.
'''

# Importing the necessary libraries
import socket # for creating and managing the sockets
import threading # for creating and handling the threads (for parallel client-server communication)
import random # for random selection of free peers during setup-dht

# The DHT manager class
class DHT_manager:
    #The constructor which initializes the required variables
    def __init__(self, manager_address, manager_port):
        self.manager_address = manager_address # setting the IP address of the DHT manager
        self.port = manager_port # setting the port number for the DHT manager to 42000
        self.peers_dict = {} # dictionary to store the peers and their respective ports
        # all the registered peers will be stored in the above dictionary in the form { <peer_name>: [<peer_ipv4>, <m_port>, <p_port>, <state_of_peer>] }
        self.dht_exists = False # boolean to check if the DHT exists or not
        self.dht_in_progress = False # boolean to check if the DHT is in progress or not in order for the manager to wait for the dht-complete command
        self.dht_teardown_in_progress = False # boolean to check if the DHT teardown is in progress or not in order for the manager to wait for the teardown-complete command
        self.dht_rebuilding_in_progress = False # boolean to check if the DHT rebuilding is in progress or not in order for the manager to wait for the dht-rebuilt command
        self.leaving_peer_name = "" # string to store the name of the peer that is leaving the DHT in order to wait for the dht-rebuilt command
        self.joining_peer_name = "" # string to store the name of the peer that is joining the DHT in order to wait for the dht-rebuilt command

    # the start method to start the DHT manager and listen for incoming connections
    def start(self):
        # creating a socket for the DHT manager
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # binding the socket to an IP address and port number
        server_socket.bind((self.manager_address, self.port))

        '''#print the IP address of the DHT manager
        print("The DHT manager is up and running on IP address " + socket.gethostbyname())'''
        # printing a message to indicate that the DHT manager has started
        print("The DHT manager is up and running on port " + str(self.port))

        # creating a thread to listen for incoming connections
        server_thread = threading.Thread(target=self.listen, args=(server_socket,))
        server_thread.start() # starting the thread
    
    # the listen method that listens for incoming connection requests
    def listen(self, server_socket):
        # The DHT manager's server thread will keep running and listening for incoming connections
        while True:
            # receive any data that is incoming from peers
            peer_data, peer_address = server_socket.recvfrom(1024)
            # decode the data to a string
            peer_data = peer_data.decode('utf-8')
            # print the data received
            print(peer_data)
            # split the data on the basis of spaces and store it in a list
            peer_data = peer_data.split(' ')
            if peer_data[0] == "dht-complete": # if the command is dht-complete
                # start a thread to handle the dht-complete command as the DHT manager can handle multiple peers at the same time
                dht_complete_thread = threading.Thread(target=self.dht_complete, args=(server_socket, peer_address, *peer_data[1:]))
                dht_complete_thread.start()
            elif peer_data[0] == "dht-rebuilt": # if the command is dht-rebuilt
                # start a thread to handle the dht-rebuilt command as the DHT manager can handle multiple peers at the same time
                dht_rebuilt_thread = threading.Thread(target=self.dht_rebuilt, args=(server_socket, peer_address, *peer_data[1:]))
                dht_rebuilt_thread.start()
            elif peer_data[0] == "teardown-complete": # if the command is teardown-complete
                # start a thread to handle the teardown-complete command as the DHT manager can handle multiple peers at the same time
                teardown_complete_thread = threading.Thread(target=self.teardown_complete, args=(server_socket, peer_address, *peer_data[1:]))
                teardown_complete_thread.start()
            # first check if the dht_in_progress or dht_teardown_in_progress or dht_rebuilding_in_progress boolean is True and if it is, wait for the dht-complete or teardown-complete command by sending "FAILURE: DHT in progress" or "FAILURE: Teardown in progress" or "FAILURE: Rebuilding in progress" to the peer and its respective m-port
            elif self.dht_in_progress or self.dht_teardown_in_progress or self.dht_rebuilding_in_progress:
                if self.dht_in_progress:
                    server_socket.sendto("FAILURE: DHT in progress".encode('utf-8'), (peer_address, int(peer_data[1])))
                elif self.dht_teardown_in_progress:
                    server_socket.sendto("FAILURE: Teardown in progress".encode('utf-8'), (peer_address, int(peer_data[1])))
                else:
                    server_socket.sendto("FAILURE: Rebuilding in progress".encode('utf-8'), (peer_address, int(peer_data[1])))
                continue
            # check the command received and call the respective method
            elif peer_data[0] == "register": # if the command is register
                # start a thread to handle the register command as the DHT manager can handle multiple peers at the same time
                register_thread = threading.Thread(target=self.register, args=(server_socket, peer_address, *peer_data[1:]))
                register_thread.start()
            elif peer_data[0] == "setup-dht": # if the command is setup-dht
                # start a thread to handle the setup-dht command as the DHT manager can handle multiple peers at the same time
                setup_dht_thread = threading.Thread(target=self.setup_dht, args=(server_socket, peer_address, *peer_data[1:]))
                setup_dht_thread.start()
            elif peer_data[0] == "query-dht": # if the command is query-dht
                # start a thread to handle the query-dht command as the DHT manager can handle multiple peers at the same time
                query_dht_thread = threading.Thread(target=self.query_dht, args=(server_socket, peer_address, *peer_data[1:]))
                query_dht_thread.start()
            elif peer_data[0] == "leave-dht": # if the command is leave-dht
                # start a thread to handle the leave-dht command as the DHT manager can handle multiple peers at the same time
                leave_dht_thread = threading.Thread(target=self.leave_dht, args=(server_socket, peer_address, *peer_data[1:]))
                leave_dht_thread.start()
            elif peer_data[0] == "join-dht": # if the command is join-dht
                # start a thread to handle the join-dht command as the DHT manager can handle multiple peers at the same time
                join_dht_thread = threading.Thread(target=self.join_dht, args=(server_socket, peer_address, *peer_data[1:]))
                join_dht_thread.start()
            elif peer_data[0] == "deregister": # if the command is deregister
                # start a thread to handle the deregister command as the DHT manager can handle multiple peers at the same time
                deregister_thread = threading.Thread(target=self.deregister, args=(server_socket, peer_address, *peer_data[1:]))
                deregister_thread.start()
            elif peer_data[0] == "teardown-dht": # if the command is teardown-dht
                # start a thread to handle the teardown-dht command as the DHT manager can handle multiple peers at the same time
                teardown_dht_thread = threading.Thread(target=self.teardown_dht, args=(server_socket, peer_address, *peer_data[1:]))
                teardown_dht_thread.start()
            
            else: # if the command is not recognized
                print("Command not recognized")
            
    def register(self, server_socket, peer_address, *args):
        # divide the arguments into peer name, IPv4 address, m-port, and p-port
        peer_name = args[0]
        peer_ipv4 = args[1]
        m_port = int(args[2])
        p_port = int(args[3])
        # check the length of peer_name (should be at most 15 characters)
        if len(peer_name) > 15:
            # checks if the length of the peer name is greater than 15 characters
            server_socket.sendto("FAILURE: Peer name should be at most 15 characters".encode('utf-8'), peer_address)
            # exit the method
            return
        
        # check if the peer name is already registered in the peers dictionary
        if peer_name in self.peers_dict:
            # checks if the peer name is already registered
            server_socket.sendto("FAILURE: Peer name is already registered".encode('utf-8'), peer_address)
            # exit the method
            return
        
        # check if the m-port and p-port are already registered in the peers dictionary
        # peer_dict stores values in the form { <peer_name>: [<peer_ipv4>, <m_port>, <p_port>, <state_of_peer>] }
        for key, value in self.peers_dict.items():
            if value[1] == m_port or value[2] == p_port:
                # checks if the m-port or p-port is already registered
                server_socket.sendto("FAILURE: m-port or p-port is already registered".encode('utf-8'), peer_address)
                # exit the method
                return
        
        # if the peer name, m-port, and p-port are not already registered, add the peer to the peers dictionary
        self.peers_dict[peer_name] = [peer_ipv4, m_port, p_port, "Free"]

        # send a success message to the peer
        server_socket.sendto("SUCCESS".encode('utf-8'), peer_address)
    
    def setup_dht(self, server_socket, peer_address, *args):
        # divide the arguments into peer name, size n, and data from year YYYY
        peer_name = args[0]
        size_n = args[1]
        data_from_year = args[2]

        # check if the peer name is registered in the peers dictionary
        if peer_name not in self.peers_dict:
            # if the peer is not registered, send a return code of FAILURE
            server_socket.sendto("FAILURE: Peer name is not registered".encode('utf-8'), peer_address)
            # exit the method
            return
        
        # check is the size_n is at least 3
        if int(size_n) < 3:
            # if the size_n is less than 3, send a return code of FAILURE
            server_socket.sendto("FAILURE: Size n should be at least 3".encode('utf-8'), peer_address)
            # exit the method
            return
        
        # check if the number of peers is at least size_n
        if len(self.peers_dict) < int(size_n):
            # if the number of peers is less than size_n, send a return code of FAILURE
            server_socket.sendto("FAILURE: Number of peers is less than size n".encode('utf-8'), peer_address)
            # exit the method
            return
        
        # check if the DHT already exists
        if self.dht_exists:
            # if the DHT already exists, send a return code of FAILURE
            server_socket.sendto("FAILURE: DHT already exists".encode('utf-8'), peer_address)
            # exit the method
            return
        
        # If all the checks pass, set the state of the peer to "Leader"
        self.peers_dict[peer_name][3] = "Leader"

        # Get a list of all the "Free" peers from the peers dictionary
        free_peers = [key for key, value in self.peers_dict.items() if value[3] == "Free"]

        # Randomly select size_n - 1 peers from the list of free peers
        free_peers = random.sample(free_peers, int(size_n) - 1)

        # Update the state of the randomly selected free_peers to "InDHT"
        for peer in free_peers:
            self.peers_dict[peer][3] = "InDHT"
        
        # create a list containing 3-tuple elements of the form (peer_name, peer_ipv4, p_port)
        # the first element of the list is the leader's 3-tuple
        dht_list = [(peer_name, self.peers_dict[peer_name][0], self.peers_dict[peer_name][2])]
        # add the 3-tuple elements of the randomly selected free_peers to the dht_list
        for peer in free_peers:
            dht_list.append((peer, self.peers_dict[peer][0], self.peers_dict[peer][2]))
        

        # set the DHT in progress boolean to True
        self.dht_in_progress = True

        # send a return code of SUCCESS and the dht_list to the leader
        print("working here")
        returncode = "SUCCESS\n" + str(dht_list)
        server_socket.sendto(returncode.encode('utf-8'), peer_address)
    
    def dht_complete(self, server_socket, peer_address, *args):
        # divide the argmuments into peer name
        peer_name = args[0]

        # checks if the peer name is registered and its state is "Leader"
        if peer_name not in self.peers_dict or self.peers_dict[peer_name][3] != "Leader":
            # if the peer name is not registered or its state is not "Leader", send a return code of FAILURE
            server_socket.sendto("FAILURE: Peer name is not registered or is not the leader".encode('utf-8'), peer_address)
            # exit the method
            return
        
        # set the DHT exists boolean to True as the DHT is now complete
        self.dht_exists = True
        # set the DHT in progress boolean to False as the DHT is now complete so the manager can now listen for incoming commands
        self.dht_in_progress = False

        # send a return code of SUCCESS to the leader
        server_socket.sendto("SUCCESS".encode('utf-8'), peer_address)

    def query_dht(self, server_socket, peer_address, *args):
        # divide the argument into peer name
        peer_name = args[0]

        # first check if the DHT has been set up (exists)
        if not self.dht_exists:
            # if the DHT has not been set up yet, send a return code of FAILURE
            server_socket.sendto("FAILURE: DHT does not exist".encode('utf-8'), peer_address)
            # exit the method
            return
        
        # check if the peer name is registered in the peers dictionary
        if peer_name not in self.peers_dict:
            # if the peer name is not registered, send a return code of FAILURE
            server_socket.sendto("FAILURE: Peer name is not registered".encode('utf-8'), peer_address)
            # exit the method
            return
        
        # check if the state of the peer is "Free" or not
        if self.peers_dict[peer_name][3] != "Free":
            # if the state of the peer is not "Free", send a return code of FAILURE
            server_socket.sendto("FAILURE: Peer is not free".encode('utf-8'), peer_address)
            # exit the method
            return
        
        # randomly select a peer which is in the DHT
        peer_in_dht = random.choice([(key, value) for key, value in self.peers_dict.items() if value[3] == "InDHT"])
        peer_in_dht = [(peer_in_dht[0], peer_in_dht[1][0], peer_in_dht[1][2])]
        # send a return code of SUCCESS and the 3-tuple of the peer_in_dht to the peer
        returncode = "SUCCESS\n" + str(peer_in_dht)
        server_socket.sendto(returncode.encode('utf-8'), peer_address)

    def leave_dht(self, server_socket, peer_address, *args):
        # divide the argument into peer name
        peer_name = args[0]

        # check if the DHT exists
        if not self.dht_exists:
            # if the DHT does not exist, send a return code of FAILURE
            server_socket.sendto("FAILURE: DHT does not exist".encode('utf-8'), peer_address)
            # exit the method
            return
        
        # check if the peer is one of the peers in the DHT
        if self.peers_dict[peer_name][3] != "InDHT":
            # if the peer is not in the DHT, send a return code of FAILURE
            server_socket.sendto("FAILURE: Peer is not in the DHT".encode('utf-8'), peer_address)
            # exit the method
            return
        
        # store the name of the leaving peer in the leaving_peer_name variable
        self.leaving_peer_name = peer_name

        # set the dht_rebuilding_in_progress boolean to True
        self.dht_rebuilding_in_progress = True

        # send a return code of SUCCESS to the peer
        server_socket.sendto("SUCCESS: Left the DHT".encode('utf-8'), peer_address)

    def join_dht(self, server_socket, peer_address, *args):
        # divide the argument into peer name
        peer_name = args[0]

        # check if the state of the peer is "Free"
        if self.peers_dict[peer_name][3] != "Free":
            # if the state of the peer is not "Free", send a return code of FAILURE
            server_socket.sendto("FAILURE: Peer is not free".encode('utf-8'), peer_address)
            # exit the method
            return
        
        # check if the DHT exists
        if not self.dht_exists:
            # if the DHT does not exist, send a return code of FAILURE
            server_socket.sendto("FAILURE: DHT does not exist".encode('utf-8'), peer_address)
            # exit the method
            return
        
        # store the name of the joining peer in the joining_peer_name variable
        self.joining_peer_name = peer_name

        # set the dht_rebuilding_in_progress boolean to True
        self.dht_rebuilding_in_progress = True

        # send a return code of SUCCESS along with the 3-tuple of the leader to the peer
        returncode = "SUCCESS\n" + str((self.peers_dict[peer_name], self.peers_dict[peer_name][0], self.peers_dict[peer_name][2]))
        server_socket.sendto(returncode.encode('utf-8'), peer_address)

    def dht_rebuilt(self, server_socket, peer_address, *args):
        # divide the arguments into peer name and new-leader
        peer_name = args[0]
        new_leader = args[1]

        # check if the peer name is not the leaving_peer_name or the joining_peer_name
        if peer_name != self.leaving_peer_name or peer_name != self.joining_peer_name:
            # if the peer name is not the leaving_peer_name or the joining_peer_name, send a return code of FAILURE
            server_socket.sendto("FAILURE: Peer name is not the leaving or joining peer".encode('utf-8'), peer_address)
            # exit the method
            return
        
        # if the peer_name is the leaving_peer_name, set the state of the peer to "Free"
        if peer_name == self.leaving_peer_name:
            self.peers_dict[peer_name][3] = "Free"
            self.leaving_peer_name = ""
        
        # if the peer_name is the joining_peer_name, set the state of the peer to "InDHT"
        if peer_name == self.joining_peer_name:
            self.peers_dict[peer_name][3] = "InDHT"
            self.joining_peer_name = ""

        # check if the new_leader is the same as the leader of the DHT
        # if not, set the state of the new_leader to "Leader" and the state of the old leader to "InDHT
        if self.peers_dict[new_leader][3] != "Leader":
            for key, value in self.peers_dict.items(): # find the old leader and set its state to "InDHT"
                if value[3] == "Leader":
                    self.peers_dict[key][3] = "InDHT"
                    break
            self.peers_dict[new_leader][3] = "Leader" # set the state of the new_leader to "Leader"
        
        # set the dht_rebuilding_in_progress boolean to False
        self.dht_rebuilding_in_progress = False

        # send a return code of SUCCESS to the peer
        server_socket.sendto("SUCCESS: DHT rebuilt".encode('utf-8'), peer_address)

    def deregister(self, server_socket, peer_address, *args):
        # divide the argument into peer name
        peer_name = args[0]

        # check if the state of the peer is "InDHT"
        if self.peers_dict[peer_name][3] == "InDHT":
            # if the state of the peer is "InDHT", send a return code of FAILURE
            server_socket.sendto("FAILURE: Peer is in a DHT".encode('utf-8'), peer_address)
            # exit the method
            return
        
        # remove the peer from the peers dictionary (deregister the peer)
        self.peers_dict.pop(peer_name)

        # send a return code of SUCCESS to the peer
        server_socket.sendto("SUCCESS: Deregistered".encode('utf-8'), peer_address)

    def teardown_dht(self, server_socket, peer_address, *args):
        # divide the argument into peer name
        peer_name = args[0]

        # check if the peer is the leader of the DHT
        if self.peers_dict[peer_name][3] != "Leader":
            # if the peer is not the leader, send a return code of FAILURE
            server_socket.sendto("FAILURE: Peer is not the leader".encode('utf-8'), peer_address)
            # exit the method
            return
        
        # set the DHT teardown in progress boolean to True
        self.dht_teardown_in_progress = True

        # send a return code of SUCCESS to the leader
        server_socket.sendto("SUCCESS: Teardown in progress".encode('utf-8'), peer_address)

    def teardown_complete(self, server_socket, peer_address, *args):
        # divide the argument into peer name
        peer_name = args[0]

        # check if the peer is the leader of the DHT
        if self.peers_dict[peer_name][3] != "Leader":
            # if the peer is not the leader, send a return code of FAILURE
            server_socket.sendto("FAILURE: Peer is not the leader".encode('utf-8'), peer_address)
            # exit the method
            return
        
        # change the state of all the peers in the DHT to "Free"
        for key, value in self.peers_dict.items():
            if value[3] == "InDHT" or value[3] == "Leader":
                self.peers_dict[key][3] = "Free"

        # set the DHT exists boolean to False as the DHT has been torn down
        self.dht_exists = False

        # set the DHT teardown in progress boolean to False as the DHT has been torn down so the manager can now listen for incoming commands
        self.dht_teardown_in_progress = False

        # send a return code of SUCCESS to the leader
        server_socket.sendto("SUCCESS: Teardown complete".encode('utf-8'), peer_address)


# the main method to create the DHT manager and start it
if __name__ == "__main__":
    # ask the user for the IP address of the DHT manager and the port number
    manager_address = input("Enter the IP address of the DHT manager: ")
    manager_port = int(input("Enter the port number of the DHT manager (42000-42499): "))
    # create the DHT manager
    dht_manager = DHT_manager(manager_address, manager_port)
    # start the DHT manager
    dht_manager.start()