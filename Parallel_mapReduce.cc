#include <iostream>
#include <fstream>
#include <mpi.h>
#include <pthread.h>
#include <string>
#include <string.h>
#include <map>
#include <vector>
#include <algorithm>
#include <cmath>
#include <ctime>
#include <cassert>
#include <chrono>
#include <unistd.h>
#include <numeric>

using std::chrono::duration_cast;using std::chrono::milliseconds;
using std::chrono::system_clock;
using namespace std;

//define data type
typedef pair<string, int> Item;
//Write to local
map<string, int> local_result;
//Use to lock
pthread_mutex_t mutex ;
//Set message
char set_sleep[6] = "sleep";
char set_finish[7] = "finsh";
char set_start[6] = "start";
//Sort the result in ascending 
int sort_asc=true;
//Record log
ofstream logfile;
//Handle loc file
map <int, int> loc_config;
//Handle chunk data
vector<string> chunk_data;


typedef struct mapper_arg{
    int thread_id;
    int rank;
    string data;
    int reducer;
}mapper_args;

typedef struct task_state{
    int have_locality;
    int sleep_yet;
    int taking_task;
}taskTracker_state;

bool cmp(const Item &a, const Item &b);
bool cmp2(const Item &a, const Item &b);
void map_function(vector<Item> &map_result, string line);
void partition(vector<Item> &map_result,map<Item, int> &hash_table, int reducer_num);
void map_function(vector<Item> &map_result, string line);
void storeToLocal(string input_data,map<string, int> &local_result);

bool cmp(const Item &a, const Item &b)
{
    return a.first < b.first;
}

bool cmp2(const Item &a, const Item &b)
{
    return a.first > b.first;
}

void map_function(vector<Item> &map_result, string line){
    string word;
    Item content;
    while(line.find(" ") != string::npos ){
        word = line.substr(0, line.find(" "));
        content = make_pair(word,1);
        map_result.push_back(content);
        line.erase(0,line.find(" ") + 1);
    }
}
void partition(vector<Item> &map_result,map<Item, int> &hash_table, int reducer_num){
    int val;
    for(int j = 0 ;j < map_result.size();j++){
        val = int(map_result[j].first[0]) % reducer_num;
        hash_table[map_result[j]] = val;
    }
}

void storeToLocal(string input_data,map<string, int> &local_result){
    size_t pointer = 0;
    string word;
    pthread_mutex_lock(&mutex);
  
  while ((pointer = input_data.find(" ")) != string::npos){
        word = input_data.substr(0, pointer);
        input_data.erase(0, pointer+1);
        if (local_result.count(word) == 0)
        {
            local_result[word] = 1;
        }
        else
        {
            local_result[word] +=1;
        }
    }

    pthread_mutex_unlock(&mutex); 
	pthread_exit(NULL);
}

void* mapper_task(void* mapper_arg){
    struct mapper_arg* arg = (struct mapper_arg*)mapper_arg;
    string input_data = arg->data;
    string input_data1 = arg->data;
    int reducer_num = arg->reducer;
    vector<Item> map_result;
    
    //map function
    map_function(map_result,input_data1);
   
    //partition function
    map<Item, int> hash_table;
    partition(map_result,hash_table,reducer_num);
   
    //Store to local
    storeToLocal(input_data,local_result);
    
}

int main(int argc, char **argv)
{
    auto start = std::chrono::system_clock::now();

    //Declare variables 
    int rank, size, rc; // MPI variable 

    //argument parsing
    assert(argc == 8);
    string job_name    = string(argv[1]);
    int num_reducer    = stoi(argv[2]);
    int delay          = stoi(argv[3]);
    string input_filename  = string(argv[4]);
    int chunk_size     = stoi(argv[5]);
    string locality_config_filename = string(argv[6]);
    string output_dir  = string(argv[7]);

    //Record log
    logfile.open(output_dir + job_name + "-" + "log.out");

    //Initial MPI
    rc = MPI_Init(&argc,&argv);
    if(rc!=MPI_SUCCESS){
        printf("Error starting MPI program. Terminating.\n");
        MPI_Abort(MPI_COMM_WORLD,rc);
    }
    MPI_Comm_rank(MPI_COMM_WORLD,&rank);//return rank id
    MPI_Comm_size(MPI_COMM_WORLD,&size);//return size of nodes

    //detect how many CPUs are available 
    cpu_set_t cpu_set;
    sched_getaffinity(0, sizeof(cpu_set), &cpu_set);
    int cpu_num=CPU_COUNT(&cpu_set);
    int worker_threads = cpu_num- 1;

    //Record log
    logfile <<duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()<<",Start_Job,"<<job_name<<","<<size<<","<<cpu_num<<","<<num_reducer<<","<<delay<<","<<input_filename<<","<<chunk_size<<","<<locality_config_filename<<","<<output_dir<<endl;
    //Jobtracker
    if(rank==0){

        //Record log
        logfile <<duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()<<",Start_Job,"<<job_name<<","<<size<<","<<cpu_num<<","<<num_reducer<<","<<delay<<","<<input_filename<<","<<chunk_size<<","<<locality_config_filename<<","<<output_dir<<endl;

        //Read the data locality config file of the input file, which contains each data chunk (chunkID) mapping to the location (nodeID).
        ifstream loc_file(locality_config_filename);
        string loc_line;
        while(getline(loc_file, loc_line)){
            int chunk_id, node_id,pointer = loc_line.find(" ");
            chunk_id = stoi(loc_line.substr(0, pointer));
            node_id = stoi(loc_line.substr(pointer,loc_line.length()));
            loc_config[chunk_id] = node_id;                                    
        
        }
        loc_file.close();
    
        // Read words file
        ifstream input_file(input_filename);
        string input_line;
        string chunk_content="";
        int cnt = 1;
        while (getline(input_file, input_line)){
            if((cnt%chunk_size)!=0){
                chunk_content += input_line+ " ";
            }else{
                chunk_content += input_line+ " ";
                chunk_data.push_back(chunk_content);
                chunk_content = "";
            }
            cnt++;
        }
        input_file.close();

        //Tasktrackers
        int taskTracker_id;
        //TaskTracker_state
        struct task_state taskTracker_state[size-1];
        //Checking which task is finish
        int task_finish[chunk_data.size()] = {0};
        //Checking which taskTracker has locality
        int taskTracker_locality[size-1];
      
        //Initial taskTracker state
        for(int id=0;id<size-1;id++){
            taskTracker_state[id].sleep_yet = 0;
            taskTracker_state[id].have_locality = 0;
            taskTracker_state[id].taking_task = 0;
        }
        
        //Listening to taskTracker
        while(true){
            //Initial taskTracker's locality 
            memset(taskTracker_locality, 0, (size-1)*sizeof(taskTracker_locality[0]));

            //Receive request from taskTracker
            MPI_Recv(&taskTracker_id, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            //Check whether taskTracker has locality
            for(int chunk_id=1;chunk_id <= loc_config.size();chunk_id++){

                //If the nodeID is larger than the number of worker nodes, mod the nodeID by the number of worker nodes.
                int changeNodeId=(loc_config[chunk_id])%(size-1)+1 ;
                if(task_finish[chunk_id-1] == 0 && changeNodeId == taskTracker_id ){
                    
                    //Record task and taskTracker state
                    taskTracker_locality[taskTracker_id-1] = true;
                    task_finish[chunk_id-1] = true;
                    //taskTracker_state[taskTracker_id-1].have_locality;

                    //Dispatch the map task
                    MPI_Send(chunk_data[chunk_id-1].c_str(), chunk_data[chunk_id-1].size(), MPI_CHAR, taskTracker_id, 0, MPI_COMM_WORLD);
                    logfile<<duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()<<",Dispatch_MapTask,"<<chunk_id<<","<<taskTracker_id<<endl;

                    break;
                }
            }
            
            //If taskTracker doesn't have locality
            if(!taskTracker_locality[taskTracker_id-1]){
       
                //Check if taskTracker sleep yet
                if(!taskTracker_state[taskTracker_id-1].sleep_yet){ 
                    MPI_Send(set_sleep, 6, MPI_CHAR, taskTracker_id, 0, MPI_COMM_WORLD);
                    taskTracker_state[taskTracker_id-1].sleep_yet = true;
                }
                else {
                    //Complete sleeping
                    for(int chunk_id=1;chunk_id <= loc_config.size();chunk_id++){

                        //Dispatch unfinish map task
                        if(!task_finish[chunk_id-1]){

                            task_finish[chunk_id-1] = true;
                            
                            //Dispatch the map tasks
                            MPI_Send(chunk_data[chunk_id-1].c_str(), chunk_data[chunk_id-1].size(), MPI_CHAR, taskTracker_id, 0, MPI_COMM_WORLD);
                            logfile<<duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()<<",Dispatch_MapTask,"<<chunk_id<<","<<taskTracker_id<<endl;

                            break;
                        }
                    }
                }
            }
            //Check whether all task are done
            int handleAllTask=1;
            for(int id=0; id<loc_config.size(); id++){
                if(task_finish[id] == 0){
                    handleAllTask=0;
                    break;
                }
            }
            if(handleAllTask)break;

        }
        for(int id=0;id<size-1;id++){
            //Get the finish message from taskTracker
            MPI_Recv(&taskTracker_id, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Send(set_finish, 7, MPI_CHAR, taskTracker_id, 0, MPI_COMM_WORLD);
           
            logfile << duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count() <<",Complete_MapTask,"<<id<<","<<taskTracker_id<<endl;
        }

    }else{ // Tasktracker
       
        vector<Item> word_sort;//For sorting the local result
        string send_data="";//For sending data

        //Listening to jobTracker
        while(true){
           
            int count;//For the size of receiving data 
            MPI_Send(&rank, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
            
            //Receive response from jobtracker 
            //Probe for an incoming message from process zero
            MPI_Status status;
            MPI_Probe(0, 0, MPI_COMM_WORLD, &status);

            //When probe returns, the status object has the size and other attributes of the incoming message. Get the message size
            //Reference: https://mpitutorial.com/tutorials/dynamic-receiving-with-mpi-probe-and-mpi-status/zh_cn/
            MPI_Get_count(&status,MPI_CHAR,&count);
            
            //Allocate a buffer to hold the incoming numbers
            //char *receive_data = (char*)malloc(sizeof(char) * count);
            char receive_data[count];

            //Now receive the message with the allocated buffer
            MPI_Recv(&receive_data, count, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            
            //Convert char array to string
            string receive_string(receive_data);

            //Distinguish next action
            if(receive_string == "finsh"){
                break;
            }
            else if(receive_string == "sleep"){
                usleep(delay * 1000000);
            }else{
                
                int cnt=0;
                int words_amount = 0;//Count the word 
                size_t pointer = 0;
                string temp_str=""; //For temporarly save word
                string temp_recv_str; //For counting the word
                temp_recv_str = receive_string;
                vector<string> thread_data; //Allocate data to thread

                // Setup pthread 
                pthread_mutex_init(&mutex, NULL);
                pthread_t threads[worker_threads];
                struct mapper_arg mapper_args[worker_threads];
    
                //Count the total word
                while ((pointer = temp_recv_str.find(" ")) != string::npos)
                {
                    temp_recv_str.erase(0, pointer+1);
                    words_amount++;
                }
                
                int thread_data_size = words_amount / worker_threads;
                int thread_data_remainer = words_amount % worker_threads;
                
                //Evenly distribute the number of threads to be processed in each node
                while ((pointer = receive_string.find(" ")) != string::npos){

                    cnt++;
                    temp_str += receive_string.substr(0, pointer)+" ";
                    receive_string.erase(0, pointer+1);
                    
                    //If there is a remainder, it will be allocated from the beginning
                    if(cnt == thread_data_size && thread_data_remainer > 0 && (pointer = receive_string.find(" ")) != string::npos){
                        //Add one word
                        temp_str += receive_string.substr(0, pointer)+" ";
                        receive_string.erase(0, pointer+1);

                        thread_data.push_back(temp_str + " ");
                        temp_str = "";
                        thread_data_remainer--;
                        cnt=0;
                    }else if(cnt==thread_data_size){
                        thread_data.push_back(temp_str + " ");
                        temp_str = "";
                        cnt=0;
                    }
                }

                for(int id=0; id<worker_threads; id++){
                    mapper_args[id].thread_id = id;
                    mapper_args[id].rank = rank;
                    mapper_args[id].data = thread_data[id];
                    mapper_args[id].reducer = rank;
                    pthread_create(&threads[id], NULL, mapper_task, (void*)&mapper_args[id]);
                }
                for(int id=0; id<worker_threads; id++){
                    pthread_join(threads[id], NULL);
                }
    
            }
        }
        
        //Set local result to another type in order to sort
        for (auto &item : local_result){
            word_sort.push_back(item);
        }
        
        //Sort the local result
        auto start_shuffle=duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        logfile<<start_shuffle<<",Start_Shuffle,"<< local_result.size() <<endl;

        if(sort_asc){
            sort(word_sort.begin(), word_sort.end(),cmp);
        }else{
            sort(word_sort.begin(), word_sort.end(),cmp2);
        }
        
        auto end_shuffle=duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        logfile<<end_shuffle<<",Finish_Shuffle,"<< end_shuffle-start_shuffle  <<endl;
        ofstream out(output_dir + job_name + "-" + to_string(rank) + ".out");

        //Turn result to string type
        for (auto &iter_ : local_result){
            if(iter_.first.empty())continue;
            send_data+= iter_.first+" ";
            send_data+= to_string(iter_.second)+" ";
        }

        if(rank!=1){

            //Receive start message
            char recv_bufff[6];
            MPI_Recv(&recv_bufff, 6, MPI_CHAR, 1, 3, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Send(send_data.c_str(), send_data.size()+1, MPI_CHAR, 1, 4, MPI_COMM_WORLD);
            logfile <<duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()<<",Dispatch_ReduceTask,"<<rank<<","<<"1"<<endl;

        }else{ //rank==1

            vector<Item> word_sort;//For sorting the local result
            size_t word_amount_file;//For calculate word amount in each file
            int reducer_id = 1;//For set file name

            //Check if there are more than 2 taskTracker
            if((size-1) >= 2){
                
                //Collect data from other taskTrackeres
                for(int tasktracker_id=2; tasktracker_id <= size-1; tasktracker_id++){
                    
                    int count;//the size of receiving data
                    int word_count;//Count each word
                    int word_site= 0;//Check word site
                    size_t pointer=0;
                    string word_key;//Get word 
                    string receive_data_string= "";//Turn receiving data to string

                    //Tell other taskTrackeres they can start
                    MPI_Send(set_start, 6, MPI_CHAR , tasktracker_id, 3, MPI_COMM_WORLD);

                    //Receive the size of receiving data
                    MPI_Status status;
                    MPI_Probe(tasktracker_id, 4, MPI_COMM_WORLD, &status);
                    MPI_Get_count(&status,MPI_CHAR,&count);

                    char receive_data[count];
                    MPI_Recv(&receive_data, count, MPI_CHAR, tasktracker_id, 4, MPI_COMM_WORLD, &status);
                    auto start_reduce=duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

                    receive_data_string = receive_data;

                    //Sum the number of all words from other task tracker
                    while ((pointer = receive_data_string.find(" ")) != string::npos){
                           
                            if(word_site%2!=0){
                                word_count = stoi(receive_data_string.substr(0, pointer));
                                if (local_result.count(word_key) == 0)
                                {
                                    local_result[word_key] = word_count;
                                }
                                else
                                {
                                    local_result[word_key] += word_count;
                                }
                            }else{
                                word_key = receive_data_string.substr(0, pointer);
                            }
                            word_site++;
                            receive_data_string.erase(0, pointer+1);
                    }

                    auto end_reduce=duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
                    logfile <<duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()<<",Complete_ReduceTask,"<<"1"<<","<<end_reduce-start_reduce<<endl;

                }
            }

            //Set local result to another type in order to sort
            for (auto &item : local_result){
                if(item.first.empty())continue;
                word_sort.push_back(item);
            }
            
            //Sort the local result
            auto start_shuffle=duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
            logfile<<start_shuffle<<",Start_Shuffle,"<< local_result.size() <<endl;

            if(sort_asc){
                sort(word_sort.begin(), word_sort.end(),cmp);
            }else{
                sort(word_sort.begin(), word_sort.end(),cmp2);
            }
        
            auto end_shuffle=duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
            logfile<<end_shuffle<<",Finish_Shuffle,"<< end_shuffle-start_shuffle  <<endl;

            //Output result
            // ofstream out(output_dir + job_name + "-" + to_string(rank) + ".out");
            //Calculate the word amount for each file
            word_amount_file = ceil((double)word_sort.size() / num_reducer);

            //Write result to output file
            for (size_t i = 0; i < word_sort.size(); i += word_amount_file)
            {
                //Output result
                ofstream output_file(output_dir + job_name + "-" + to_string(reducer_id) + ".out");
                
                //Set size of output data
                size_t file_size = min(i + word_amount_file, local_result.size());
                vector<Item> file_data(word_sort.begin() + i, word_sort.begin() + file_size);

                for (int j = 0; j < file_data.size(); j++)
                {
                    if(file_data.at(j).first.empty())continue;
                    output_file << file_data.at(j).first << " " << file_data.at(j).second << "\n";
                }
                reducer_id++;
            }

            auto end = std::chrono::system_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            cout << "rank : "<< rank << " Elapsed Time(ms):" << elapsed.count() << endl;
            logfile <<duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()<<",Finish_Job,"<<elapsed.count()/1000<<endl;
    
        }
    }
    pthread_exit(NULL);
    MPI_Finalize();
    logfile.close();
    return 0;
}