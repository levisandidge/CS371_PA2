/*
# Copyright 2025 University of Kentucky
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0
*/

/* 
Please specify the group members here
# Student #1: Brett Carson
# Student #2: Levi Sandidge
# Student #3: Leighanne Lyvers
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <pthread.h>
#include <stdbool.h>
#include <errno.h>

#define MAX_EVENTS 64
#define MESSAGE_SIZE 16
#define DEFAULT_CLIENT_THREADS 4
#define TIMEOUT_SEC 1
#define MAX_RETRIES 50   // Task 2: Allow several retransmission attempts for packets
#define FRAME_DATA_SIZE 16

// Sequence number is always a 0 or a 1 
#define MAX_SEQ 2  

// Task 2: Structure for the client ID number, sequence number, ack status, and data in a frame
typedef struct {
    int client_id;                   
    int seq;                        
    int ack;                         
    char data[FRAME_DATA_SIZE];     
} frame_t;

char *server_ip = "127.0.0.1";
int server_port = 12345;
int num_client_threads = DEFAULT_CLIENT_THREADS;
int num_requests = 1000000;

// Structure to store data for client threads
typedef struct {
    int socket_fd;
    int thread_number; 
    int client_id;
} client_thread_data_t;


// Task 2: Function to determine the next sequence number (if 0, change to 1, and if 1, change to 0)
int next_seq(int current) 
{
    return (current + 1) % MAX_SEQ;
}


/*
 * This function runs in a separate client thread to handle communication with the server
 */
void *client_thread_func(void *arg) 
{
    // Setup data struct and server address structure
    client_thread_data_t *client_thread_data = (client_thread_data_t *)arg;
    struct sockaddr_in server_addr;
    socklen_t addr_len = sizeof(server_addr);

    // Initialize server family, port, and address
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);
    server_addr.sin_addr.s_addr = inet_addr(server_ip);

    // Initialize counters for packets send, packets received, and packets lost
    int tx_cnt = 0;
    int rx_cnt = 0;
    int lost_pkt_cnt = 0;

    // Create timer structures to determine when packet loss has occurred
    struct timeval request_time_start, request_time_end;
    struct timeval timeout;

    // Task 2: Setup frame structure to handle message transmission and proactively detect packet loss
    frame_t frame;
    memset(&frame, 0, sizeof(frame));  
    frame.client_id = client_thread_data->thread_number;

    // Task 2: UDP-based Sequence Numbers (SN) will start at 0 (always 0 or 1)
    int seq_num = 0;  

    // For each request from the client to the server,
    for (int request_index = 0; request_index < num_requests; request_index++) 
    {
        // Task 2: If packet loss occurs, message must be resent, so track the number of retries for a certain message
        int num_retries = 0;

        // Task 2: All messages start out as not yet acknowledged
        bool acknowledged = false;

        // Task 2: Set the frame sequence number to the current sequence number, and frame acknowledgement to -1 (not yet acknowledged)
        frame.seq = seq_num;  
        frame.ack = -1;       

        // Task 2: Before frame is sent, copy message data into frame
        memcpy(frame.data, "ABCDEFGHIJKLMNOP", FRAME_DATA_SIZE);

        // Task 2: Keep trying to send the frame until it is either acknowledged or maximum retries is reached
        while (!acknowledged && num_retries < MAX_RETRIES) 
        {
            // Send the frame to the server. If frame fails to send,
            if (sendto(client_thread_data->socket_fd, &frame, sizeof(frame), 0, (struct sockaddr *)&server_addr, addr_len) == -1) 
            {
                // Task 2: Report error in sending, increment number of retries for packet sending, and try again
                perror("sendto failed");
                num_retries++;
                continue;
            }

            // Increment the number of packets sent (Note: some packets must be resent to prevent packet loss, so this number
            // represents all calls to sendto, including resends. Packet loss is calculated differently now, not using tx_cnt - rx_cnt)
            tx_cnt++;

            // Create a set of file descriptors for reading data, since we are only using one socket for the server
            // Set the file descriptors as empty, and add client socket's file descriptor as one of descriptors to be monitored
            fd_set read_fds;
            FD_ZERO(&read_fds);
            FD_SET(client_thread_data->socket_fd, &read_fds);

            // Task 2: Wait for ACK from server using select(). Wait for up to 1 second before timeout occurs
            timeout.tv_sec = TIMEOUT_SEC;
            timeout.tv_usec = 0;

            // Monitor the socket for readability, blocking until either data is available to read, or timeout occurs after one second
            int num_ready_fds = select(client_thread_data->socket_fd + 1, &read_fds, NULL, NULL, &timeout);

            // If there is data to read after the server has provided ACK,
            if (num_ready_fds > 0 && FD_ISSET(client_thread_data->socket_fd, &read_fds)) 
            {
                // Task 2: Read data from the ACK frame
                frame_t ack_frame;
                int bytes_received = recvfrom(client_thread_data->socket_fd, &ack_frame, sizeof(ack_frame), 0, NULL, NULL);

                // Task 2: If at least a full frame of data was received
                if (bytes_received >= (int)sizeof(ack_frame)) 
                {
                    // Task 2: If the ACK is from the expected client thread and matches the current sequence number
                    if (ack_frame.client_id == client_thread_data->thread_number && ack_frame.ack == seq_num) 
                    {
                        // Task 2: ACK is valid, so packet was received successfully. Increment number of received packets
                        rx_cnt++;

                        // Task 2: Mark acknowledged boolean as true to stop retry loop, and toggle sequence number (0 or 1)
                        acknowledged = true;
                        seq_num = next_seq(seq_num);  
                    }
                }
            } 
            // Otherwise (packet loss occurred)
            else 
            {
                // Task 2: Try to send the packet again, with a small sleep to avoid overwhelming the server
                num_retries++;
                usleep(1000);  
            }
        }

        // Task 2: If maximum retries have occurred and packet was still not acknowledged,
        if (!acknowledged) 
        {
            // Increment the lost packet count
            lost_pkt_cnt++;
        }
    }

    // All messages have been sent, so we can close the socket and report data
    close(client_thread_data->socket_fd);

    // Report the number of packets lost for each client thread
    printf("Client thread %d: %d packets lost (out of %d sent)\n", client_thread_data->thread_number, lost_pkt_cnt, rx_cnt);

    return NULL;
}



/*
 * This function orchestrates multiple client threads to send requests to a server,
 * collect performance data of each thread, and compute aggregated metrics.
 */
void run_client() 
{
    // Set up the client threads that will send requests to the server
    pthread_t threads[num_client_threads];
    client_thread_data_t thread_data[num_client_threads];

    // Report that client is connecting to server and report server's IP address and port number
    printf("Client connecting to server %s:%d...\n", server_ip, server_port);

    // For each thread,
    for (int i = 0; i < num_client_threads; i++) 
    {
        // Establish UDP socket (no connection needed for UDP)
        thread_data[i].socket_fd = socket(AF_INET, SOCK_DGRAM, 0);

        // If socket creation fails, report issue
        if (thread_data[i].socket_fd < 0) 
        {
            perror("socket creation failed");
            exit(EXIT_FAILURE);
        }

        // Identify the thread number
        thread_data[i].thread_number = i;  

        // Create a new thread to pass thread-specific data
        if (pthread_create(&threads[i], NULL, client_thread_func, &thread_data[i]) != 0) 
        {
            // If thread creation failed, report error
            perror("pthread_create failed");
            exit(EXIT_FAILURE);
        }
    }

    // For each client thread,
    for (int i = 0; i < num_client_threads; i++) 
    {
        // Wait for all threads to finish
        pthread_join(threads[i], NULL);
    }
}


/*
 * Server function to handle client frames and send back ACKs.
 */
void run_server() 
{
    // Report that server is being run, and provide information pertaining to its IP address and port
    printf("Server started on %s:%d\n", server_ip, server_port);

    // Initialize a file descriptor for the server's UDP socket (only one socket for the server), and create a struct 
    // to hold the server's IP address and port, as well as the addresses of clients. Also need size of client address for recvfrom
    int server_socket_fd;
    struct sockaddr_in server_address, client_address;
    socklen_t client_len = sizeof(client_address);

    // Create a UDP socket for the server. If the file descriptor is -1, socket creation failed
    server_socket_fd = socket(AF_INET, SOCK_DGRAM, 0); 
    if (server_socket_fd == -1) 
    {
        perror("server socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Create structure for server address
    memset(&server_address, 0, sizeof(server_address));
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = INADDR_ANY;
    server_address.sin_port = htons(server_port);

    // Bind the server socket to the corresponding server address, then wait for datagrams (if binding fails, report error)
    if (bind(server_socket_fd, (struct sockaddr*)&server_address, sizeof(server_address)) == -1) 
    {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    // Report that the server is awaiting data frames from clients
    printf("Server started, waiting for frames...\n");

    // Create a buffer for the received message
    char buffer[MESSAGE_SIZE];

    // Task 2: Array for tracking the expected sequence number for each client (up to 1000 clients), initialize expected as 0 for all clients
    int expected_seq_num[1000];
    memset(expected_seq_num, 0, sizeof(expected_seq_num));  

    // While server is running,
    while (1) 
    {
        // Task 2: Receive a frame from a client using recvfrom
        frame_t frame;
        int bytes_received = recvfrom(server_socket_fd, &frame, sizeof(frame), 0, (struct sockaddr*)&client_address, &client_len);

        // Task 2: If more data is received than the size of the frame, then the full message was sent,
        if (bytes_received >= (int)sizeof(frame)) 
        {

            // Task 2: Determine the client_id and the expected sequence number for the client
            int client_num = frame.client_id;
            int expected = expected_seq_num[client_num];

            // Print the received frame info
            printf("Received frame from client %d: seq_num=%d, data=%s\n", client_num, frame.seq, frame.data); // Fixme?

            // Task 2: If the received frame's sequence number matches its expected sequence number,
            if (frame.seq == expected) 
            {
                // Task 2: Process the message and update the expected_seq_number (toggle between 0 and 1)
                expected_seq_num[client_num] = next_seq(expected);
            }

            // Task 2: Whether the sequence was correct or not, send the ack_frame to the client
            frame_t ack_frame;
            memset(&ack_frame, 0, sizeof(ack_frame));

            // Task 2: Set the client ID and sequence number for the ACK frame
            ack_frame.client_id = client_num;
            ack_frame.ack = frame.seq;

            // Task 2: Send the ACK frame to the client
            sendto(server_socket_fd, &ack_frame, sizeof(ack_frame), 0, (struct sockaddr*)&client_address, client_len);
        }
    }
    // Close the server socket file descriptor when server is shut down
    close(server_socket_fd);
}


int main(int argc, char *argv[]) 
{
    if (argc > 1 && strcmp(argv[1], "server") == 0) 
    {
        if (argc > 2) server_ip = argv[2];
        if (argc > 3) server_port = atoi(argv[3]);
        run_server();
    } 
    else if (argc > 1 && strcmp(argv[1], "client") == 0) 
    {
        if (argc > 2) server_ip = argv[2];
        if (argc > 3) server_port = atoi(argv[3]);
        if (argc > 4) num_client_threads = atoi(argv[4]);
        if (argc > 5) num_requests = atoi(argv[5]);
        run_client();
    } 
    else 
    {
        printf("Usage: %s <server|client> [server_ip server_port num_client_threads num_requests]\n", argv[0]);
    }

    return 0;
}