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
#include <sys/types.h> 

#define MAX_EVENTS 64
#define MESSAGE_SIZE 16
#define DEFAULT_CLIENT_THREADS 4

char *server_ip = "127.0.0.1";
int server_port = 12345;
int num_client_threads = DEFAULT_CLIENT_THREADS;
int num_requests = 1000000;

// Structure to store data for client threads
typedef struct {
    int epoll_fd;
    int socket_fd;
    long long total_rtt;
    long total_messages;
    float request_rate;
    int thread_number;  // We want to determine the packet loss for each individual thread
    int tx_cnt; // Task 1 - Number of packets sent
    int rx_cnt; // Task 1 - Number of packets received
    struct sockaddr_in server_addr;
} client_thread_data_t;


/*
 * This function runs in a separate client thread to handle communication with the server
 */
void *client_thread_func(void *arg) 
{
    // Setup data struct, message being sent by the client, and time evaluations of message being sent and received
    client_thread_data_t *client_thread_data = (client_thread_data_t *)arg;
    char message_to_send[MESSAGE_SIZE] = "ABCDEFGHIJKLMNOP"; 
    char received_message[MESSAGE_SIZE];
    struct timeval request_time_start, request_time_end;

    // Task 1: Need server address structure for UDP
    struct sockaddr_in server_addr;
    socklen_t addr_len = sizeof(server_addr);
    memset(&server_addr, 0, sizeof(server_addr));

    // Task 1: Initialize server family, port, and address
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);
    server_addr.sin_addr.s_addr = inet_addr(server_ip);

    // Task 1: Initialize counters for packets send, packets received, and packets lost
    int tx_cnt = 0;
    int rx_cnt = 0;
    int lost_pkt_cnt = 0;  


    // Report that a client thread has started, and the number of requests it will send
    // Fixme? printf("Client thread started to send %d requests...\n", num_requests);

    // For each request from the client to the server,
    for (int request_index = 0; request_index < num_requests; request_index++) 
    {
        // Record timestamp of start of request so we can calculate RTT later, then send the message (report error if send fails)
        gettimeofday(&request_time_start, NULL);

        // Task 1: Use sendto instead of send for sending the message
        if (sendto(client_thread_data->socket_fd, message_to_send, MESSAGE_SIZE, 0, (struct sockaddr *)&server_addr, addr_len) == -1) 
        {
            perror("message send failed");
            exit(EXIT_FAILURE);
        }

        // Task 1: increment number of packets sent
        tx_cnt++;

        // Report the message sent by the client thread
        //Fixme? printf("Client thread sent message %d\n", request_index + 1);


        // Task 1: Create a set of file descriptors for reading data, since we are only using one socket for the server
        // Set the file descriptors as empty, and add client socket's file descriptor as one of descriptors to be monitored
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(client_thread_data->socket_fd, &read_fds);

        // Task 1: Create a struct to manage the timeout time to determine when packet loss has occurred. Select() will wait up to 1 second
        struct timeval timeout;
        timeout.tv_sec = 1;  
        timeout.tv_usec = 0;

        // Task 1: Monitor the socket for readability, blocking until either data is available to read, or timeout occurs after one second
        int num_ready_fds = select(client_thread_data->socket_fd + 1, &read_fds, NULL, NULL, &timeout);

        // Task 1: If select() returns that data is available to read, and the socket is in the set of file descriptors to monitor,
        if (num_ready_fds > 0 && FD_ISSET(client_thread_data->socket_fd, &read_fds)) 
        {
            // While there are still more bytes to receive from the server (16 total)
            int total_bytes_received = 0;
            while (total_bytes_received < MESSAGE_SIZE) 
            {
                // Task 1: Use recvfrom instead of recv to receive data with parameters of the corresponding socket, a pointer to
                // the index in the buffer where data will be stored, the max number of bytes, and 0 (no special options)
                int bytes_received_from_server = recvfrom(client_thread_data->socket_fd, received_message + total_bytes_received, MESSAGE_SIZE - total_bytes_received, 0, NULL, NULL);

                // If data was received, append to received message
                if (bytes_received_from_server > 0) 
                {
                    total_bytes_received += bytes_received_from_server;
                } 
                // If no data was received, connection with the server must have failed
                else if (bytes_received_from_server == 0) 
                {
                    printf("Connection with the server failed.\n");
                    break;
                } 
                // Otherwise, the call to recvfrom function failed
                else 
                {
                    perror("recvfrom function failed");
                    break;
                }

                // If we received the full message,
                if (total_bytes_received == MESSAGE_SIZE) 
                {
                    // Task 1: Increment the count of received packets
                    rx_cnt++;

                    // Calculate the RTT using gettimeofday difference between time sent and time received in microseconds
                    gettimeofday(&request_time_end, NULL);
                    long long round_trip_time = (request_time_end.tv_sec - request_time_start.tv_sec) * 1000000LL + (request_time_end.tv_usec - request_time_start.tv_usec);
                    client_thread_data->total_rtt += round_trip_time;
                    client_thread_data->total_messages++;

                    // Report that the message was received successfully
                    // Fixme? printf("Client thread received message %d, RTT = %lld us\n", request_index + 1, round_trip_time);
                }
                // If the full message was not received,
                else 
                {
                    // Task 1: Increment the lost packet count
                    lost_pkt_cnt++;
                    //Fixme? printf("Packet lost for message %d\n", request_index + 1);
                }
            }
        }
        else
        {
            // Task 1: If epoll wait timed out, then the packet was lost, so report this and increment the lost packet count
            // Fixme? printf("Timeout: No response from the server for message %d\n", request_index + 1);
            lost_pkt_cnt++;  
        }
    }

    // Now that all requests from client to server have been made, we can calculate request rate 
    // If at least one message was sent, calculate request rate as number of messages divided by total RTT in seconds
    if (client_thread_data->total_messages > 0 && client_thread_data->total_rtt > 0) 
    {
        client_thread_data->request_rate = (double)client_thread_data->total_messages / (client_thread_data->total_rtt / 1000000.0);
    } 
    else 
    {
        // If no messages were sent, report request rate as 0
        client_thread_data->request_rate = 0.0;
    }

    // All messages have been sent, so we can close the socket and report client thread request rate
    close(client_thread_data->socket_fd);

    // Task 1: Calculate the number of lost packets for the thread, and report it, as well as the request rate for the thread
    lost_pkt_cnt = tx_cnt - rx_cnt;
    printf("Client thread %d: %d packets lost (out of %d sent)\n", client_thread_data->thread_number, lost_pkt_cnt, tx_cnt);
    // Fixme? printf("Client thread %d: %d packets lost (out of %d sent), request rate = %.2f messages/s\n", client_thread_data->thread_number, lost_pkt_cnt, tx_cnt, client_thread_data->request_rate);

    return NULL;
}



/*
 * This function orchestrates multiple client threads to send requests to a server,
 * collect performance data of each threads, and compute aggregated metrics of all threads.
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
        // Task 1: Establish UDP socket (no connection needed for UDP)
        thread_data[i].socket_fd = socket(AF_INET, SOCK_DGRAM, 0);

        // Task 1: If socket creation fails, report issue
        if (thread_data[i].socket_fd == -1) 
        {
            perror("socket creation failed");
            exit(EXIT_FAILURE);
        }

        // Identify the thread number and initialize its thread statistics to be 0
        thread_data[i].thread_number = i;
        thread_data[i].total_rtt = 0;
        thread_data[i].total_messages = 0;
        thread_data[i].request_rate = 0.0;

        // Create a new thread to pass thread-specific data
        if (pthread_create(&threads[i], NULL, client_thread_func, &thread_data[i]) != 0) 
        {
            // If thread creation failed, report error
            perror("pthread_create failed");
            exit(EXIT_FAILURE);
        }
    }

    // Initialize data to be 0 for the total RTT, messages, and request rate across all threads
    long long total_rtt = 0;
    long total_messages = 0;
    float total_request_rate = 0.0;

    // For each client thread,
    for (int i = 0; i < num_client_threads; i++) 
    {
        // Wait for all threads to finish so we can calculate data across all threads
        pthread_join(threads[i], NULL);

        // Calculate the total RTT, messages, and request rate by accumulating across all threads
        total_rtt += thread_data[i].total_rtt;
        total_messages += thread_data[i].total_messages;
        total_request_rate += thread_data[i].request_rate;
    }

    // Report all results for the client
    //Fixme? printf("Client completed. Aggregated results:\n");
    
    // Print request rate(), RTT, and the validation check for all threads
    for (int m = 0; m < num_client_threads; m++) 
    {
        // Calculate the round trip time for an individual 
        double Tj = (double)thread_data[m].total_rtt / thread_data[m].total_messages;

        // Calculate the request rate
        double RPSj = thread_data[m].request_rate;

        // Multiply the individual values for the validation check, Tj needs to be in seconds
        // the result should be 1
        double validation_check = RPSj * Tj / 1e6;

        // Converted Tj back to micro seconds so something would appear
        // Fixme? printf("Thread %d: RPS = %.6f messages/s, RTT = %.6f us, Validation = %.8f\n", m, RPSj, Tj, validation_check);
    }

    // If message(s) were sent, calculate the average RTT and report it
    long long average_rtt = 0;
    if (total_messages > 0) 
    {
        // Average round trip time is determined by the total rtt divided by number of messages sent
        average_rtt = total_rtt / total_messages;
    }
    // Fixme? printf("Average RTT: %lld us\n", average_rtt);

    // Also, report the total request rate of the client
    // Fixme? printf("Total Request Rate: %f messages/s\n", total_request_rate);
}

/*
 * Server function to handle client connections and echo messages.
 */
void run_server() 
{
    // Report that server is being run, and provide information pertaining to its IP address and port
    printf("Server started on %s:%d\n", server_ip, server_port);

    // Task 1: Initialize a file descriptor for the server's UDP socket (only one socket for the server), and create a struct 
    // to hold the server's IP address and port, as well as the addresses of clients. Also need size of client address for recvfrom
    int server_socket_fd;
    struct sockaddr_in server_address, client_address;
    socklen_t client_len = sizeof(client_address);

    // Create a UDP socket for the server. If the file descriptor is -1, socket creation failed
    server_socket_fd = socket(AF_INET, SOCK_DGRAM, 0); 
    if (server_socket_fd == -1) 
    {
        // Report error in server socket creation
        perror("Failed to create server socket connection");
        exit(EXIT_FAILURE);
    }

    // Task 1: Create structure for server address
    memset(&server_address, 0, sizeof(server_address));
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = INADDR_ANY;
    server_address.sin_port = htons(server_port);


    // Task 1: Bind the server socket to the corresponding server address, then wait for datagrams (if binding fails, report error)
    if (bind(server_socket_fd, (struct sockaddr*)&server_address, sizeof(server_address)) == -1) 
    {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    // Report that the server is awaiting a connection with a client
    printf("Server waiting on datagrams from clients...\n");

    // Create a buffer for the received message
    char buffer[MESSAGE_SIZE];

    // Task 1: UDP is connectionless, so logic for receiving messages is different
    while (1) 
    {
        // Task 1: Specify the length of the client address for UDP
        socklen_t client_address_length = sizeof(client_address);

        // Task 1: Use recvfrom to receive data from clients
        int bytes_received = recvfrom(server_socket_fd, buffer, MESSAGE_SIZE, 0, (struct sockaddr *)&client_address, &client_len);

        // If data is received,
        if (bytes_received > 0) 
        {
            // Task 1: Use %.*s to report only the data received by the server (no garbage bytes)
            printf("Server received data: %.*s\n", bytes_received, buffer);

            // Task 1: Use sendto instead of send for UDP
            sendto(server_socket_fd, buffer, bytes_received, 0, (struct sockaddr *)&client_address, client_len);

            // Report the message that the server echoes back
            //Fixme? printf("Server echoed message: %.16s\n", buffer);
        } 
        else 
        {
            // If no data was received (or recvfrom failed), then report this
            perror("recvfrom failed or no data received");
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