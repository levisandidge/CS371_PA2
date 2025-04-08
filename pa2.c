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
#include <sys/epoll.h>
#include <sys/types.h>

#define MAX_EVENTS 64
#define MESSAGE_SIZE 16
#define DEFAULT_CLIENT_THREADS 4

char *server_ip = "127.0.0.1";
int server_port = 12345;
int num_client_threads = DEFAULT_CLIENT_THREADS;
int num_requests = 1000000;

/*
 * This structure is used to store per-thread data in the client
 */
typedef struct {
    int epoll_fd;
    int socket_fd;
    long long total_rtt;
    long total_messages;
    float request_rate;
} client_thread_data_t;

/*
 * This function runs in a separate client thread to handle communication with the server
 */
void *client_thread_func(void *arg) 
{
    client_thread_data_t *client_thread_data = (client_thread_data_t *)arg;
    struct epoll_event epoll_event_to_register, triggered_events[MAX_EVENTS];
    char message_to_send[MESSAGE_SIZE] = "ABCDEFGHIJKLMNOP"; 
    char received_message[MESSAGE_SIZE];
    struct timeval request_start_time, request_end_time;


    // Monitor events for incoming data, associated with corresponding socket, and register the "connected"
    // client_thread's socket in its epoll instance
    epoll_event_to_register.events = EPOLLIN;
    epoll_event_to_register.data.fd = client_thread_data->socket_fd;

    // If epoll control fails, report error
    if (epoll_ctl(client_thread_data->epoll_fd, EPOLL_CTL_ADD, client_thread_data->socket_fd, &epoll_event_to_register) == -1) 
    {
        perror("epoll_ctl failed");
        exit(EXIT_FAILURE);
    }

    // Report that a client thread has started, and the number of requests it will send
    printf("Client thread started to send %d requests...\n", num_requests);

    // For each request from the client to the server,
    for (int request_index = 0; request_index < num_requests; request_index++) 
    {
        // Record timestamp of start of request so we can calculate RTT later, then send the message (report error if send fails)
        gettimeofday(&request_start_time, NULL);

        if (send(client_thread_data->socket_fd, message_to_send, MESSAGE_SIZE, 0) == -1) 
        {
            perror("message send failed");
            exit(EXIT_FAILURE);
        }

        // Report the message sent by the client thread
        printf("Client thread sent message %d\n", request_index + 1);

        // Wait for a response from the server, and if the response is from the correct socket,
        int number_of_triggered_events = epoll_wait(client_thread_data->epoll_fd, triggered_events, MAX_EVENTS, -1);
        if (number_of_triggered_events > 0 && triggered_events[0].data.fd == client_thread_data->socket_fd) 
        {
            // While there are still more bytes to receive from the server (16 total)
            int total_bytes_received = 0;
            while (total_bytes_received < MESSAGE_SIZE) 
            {
                // Receive data using recv function with parameters of the corresponding socket, a pointer to
                // the index in the buffer where data will be stored, the max number of bytes, and 0 (no special options)
                int bytes_received_from_server = recv(client_thread_data->socket_fd, received_message + total_bytes_received, MESSAGE_SIZE - total_bytes_received, 0);

                // If there is still data to receive, append to received message
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
                // Otherwise, the call to recv function failed
                else 
                {
                    perror("recv function failed");
                    break;
                }
            }

            // Obtain end timestamp for purpose of calculating round-trip time
            gettimeofday(&request_end_time, NULL);

            // Calculate the RTT in microseconds (the time for the message to go from client to server back to client)
            long long round_trip_time = (request_end_time.tv_sec - request_start_time.tv_sec) * 1000000LL + (request_end_time.tv_usec - request_start_time.tv_usec);

            // Add the RTT to the total RTT for the request and increment the message counter
            client_thread_data->total_rtt += round_trip_time;
            client_thread_data->total_messages++;

            // Print statement to report that the round-trip has been completed (client received message)
            printf("Client thread received message %d, RTT = %lld us\n", request_index + 1, round_trip_time);
        }
    }


    // Now that all requests from client to server have been made, we can calculate request rate 
    // If at least one message was sent, calculate request rate
    if (client_thread_data->total_messages > 0) 
    {
        // Request rate = number of messages divided by total RTT in seconds (or 0 if total_RTT = 0)
        if (client_thread_data->total_rtt > 0) 
        {
            client_thread_data->request_rate = (double)client_thread_data->total_messages / (client_thread_data->total_rtt / 1000000.0);
        } 
        else 
        {
            client_thread_data->request_rate = 0.0;
        }
        
    } 
    else 
    {
        // If no messages were sent, the request rate is 0
        client_thread_data->request_rate = 0;
    }

    // All messages have been sent, so we can close the socket and report client thread request rate
    close(client_thread_data->socket_fd);
    close(client_thread_data->epoll_fd);
    printf("Client thread finished with request rate = %.2f messages/s\n", client_thread_data->request_rate);
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
    struct sockaddr_in server_addr;

    // Set up structure for server address
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);
    inet_pton(AF_INET, server_ip, &server_addr.sin_addr);

    // Report that the client is connecting to the server and give server details
    printf("Client connecting to server %s:%d...\n", server_ip, server_port);

    // For each thread,
    for (int i = 0; i < num_client_threads; i++) 
    {
        // Establish TCP connection with the server and register socket with epoll
        thread_data[i].socket_fd = socket(AF_INET, SOCK_STREAM, 0);

        // If socket creation fails, report issue
        if (thread_data[i].socket_fd == -1) 
        {
            perror("socket creation failed");
            exit(EXIT_FAILURE);
        }

        // Create epoll instance for the thread
        thread_data[i].epoll_fd = epoll_create1(0);

        // If the epoll instance is not created properly, report the error
        if (thread_data[i].epoll_fd == -1) 
        {
            perror("epoll instance failed");
            exit(EXIT_FAILURE);
        }

        // Initialize relevant data for each thread
        thread_data[i].total_rtt = 0;
        thread_data[i].total_messages = 0;
        thread_data[i].request_rate = 0.0;

        // Establish a connection between the client's socket and the server using server address. If connection failed, report issue
        if (connect(thread_data[i].socket_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) == -1) 
        {
            perror("connection failed");
            exit(EXIT_FAILURE);
        }

        // Create a new thread to pass thread-specific data
        pthread_create(&threads[i], NULL, client_thread_func, &thread_data[i]);
    }

    // Initialize data for total RTT, messages, and request rate (as opposed to thread-specific data)
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
    printf("Client completed. Aggregated results:\n");
    
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
        printf("Thread %d: RPS = %.6f messages/s, RTT = %.6f us, Validation = %.8f\n",
            m, RPSj, Tj, validation_check);
    }


    // If message(s) were sent, calculate the average RTT and report it
    long long average_rtt = 0;
    if (total_messages > 0) 
    {
        // Average round trip time is determined by the total rtt divided by number of messages sent
        average_rtt = total_rtt / total_messages;
    }
    printf("Average RTT: %lld us\n", average_rtt);

    // Also, report the total request rate of the client
    printf("Total Request Rate: %f messages/s\n", total_request_rate);
}

/*
 * Server function to handle client connections and echo messages.
 */
void run_server() 
{
    // Run the server, and report server-specific details
    printf("Server started on %s:%d\n", server_ip, server_port);

    // Create structures to store the IP address and port number of the server and client
    struct sockaddr_in server_address;
    struct sockaddr_in client_address;

    // Create integers to store the file descriptors for the listening socket and epoll instance
    int listening_socket_file_descriptor, epoll_instance_file_descriptor;

    // Create TCP socket for IPv4 communication, which will correspond to listening socket fd
    listening_socket_file_descriptor = socket(AF_INET, SOCK_STREAM, 0);

    // Create epoll instance to monitor the created file descriptors
    epoll_instance_file_descriptor = epoll_create1(0);

    // Create structure of server address
    memset(&server_address, 0, sizeof(server_address));
    server_address.sin_family = AF_INET;

    // Server accepts connections from any address (also set the server port)
    server_address.sin_addr.s_addr = INADDR_ANY;
    server_address.sin_port = htons(server_port);

    // Bind the listening socket to corresponding server address, then listen for connections (if binding fails, report error)
    if (bind(listening_socket_file_descriptor, (struct sockaddr*)&server_address, sizeof(server_address)) == -1) 
    {
        perror("binding failed");
        exit(EXIT_FAILURE);
    }
    // SOMAXCONN is the maximum number of pending connections that can be queued by the kernel
    listen(listening_socket_file_descriptor, SOMAXCONN);

    // Use epoll and listen to wait for incoming data
    struct epoll_event listening_socket_event;
    listening_socket_event.events = EPOLLIN;
    listening_socket_event.data.fd = listening_socket_file_descriptor;

    // Register listening_socket_file_descriptor with created epoll instance
    epoll_ctl(epoll_instance_file_descriptor, EPOLL_CTL_ADD, listening_socket_file_descriptor, &listening_socket_event);

    // Report that the server is awaiting a connection with a client
    printf("Server listening for connections...\n");

    // Infinite loop to handle events
    while (1) 
    {
        // Use epoll to wait for upcoming event
        struct epoll_event triggered_events[MAX_EVENTS];
        int n = epoll_wait(epoll_instance_file_descriptor, triggered_events, MAX_EVENTS, -1);

        // For each event returned by epoll_wait
        for (int i = 0; i < n; i++) 
        {
            // If the event is on the listening socket
            if (triggered_events[i].data.fd == listening_socket_file_descriptor) 
            {
                // Determine length of client, and accept connection (if fail to accept, report error)
                socklen_t client_address_length = sizeof(client_address);
                int client_connection_file_descriptor = accept(listening_socket_file_descriptor, (struct sockaddr*)&client_address, &client_address_length);
                if (client_connection_file_descriptor == -1) 
                {
                    perror("failed to accept connection");
                    continue;
                }

                // Wait for data from client using client_connection_file_descriptor
                struct epoll_event client_connection_event;
                client_connection_event.events = EPOLLIN;
                client_connection_event.data.fd = client_connection_file_descriptor;

                // Register client file descriptor with epoll instance (if epoll control fails, report error)
                if (epoll_ctl(epoll_instance_file_descriptor, EPOLL_CTL_ADD, client_connection_file_descriptor, &client_connection_event) == -1) 
                {
                    perror("epoll control failed");
                    close(client_connection_file_descriptor);
                    continue;
                }

                // Report new client connection
                printf("Server accepted new client connection.\n");
            }
            else 
            {
                // Handle incoming data from client, echo received data back to the client
                char received_data_buffer[MESSAGE_SIZE];
                int ret = recv(triggered_events[i].data.fd, received_data_buffer, MESSAGE_SIZE, 0);
                if (ret > 0) 
                {
                    // Echo data received from client back to client
                    send(triggered_events[i].data.fd, received_data_buffer, ret, 0);

                    // Report the echoed message
                    printf("Server echoed message: %.16s\n", received_data_buffer);
                }
                else 
                {
                    // If client closed connection, close corresponding socket and remove from epoll
                    close(triggered_events[i].data.fd);
                    epoll_ctl(epoll_instance_file_descriptor, EPOLL_CTL_DEL, triggered_events[i].data.fd, NULL);
                }
            }
        }
    }
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
