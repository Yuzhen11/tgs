#ifndef WORKEROL_H
#define WORKEROL_H

#include "global_ol.h"
#include "utils/global.h"
#include "MessageBufferOL.h"
#include <string>
#include "utils/communication.h"
#include "utils/ydhdfs.h"
#include "hdfs.h"
#include "utils/Combiner.h"
#include "tools/msgtool.h"
#include "utils/Aggregator.h"//for DummyAgg


#include <unistd.h>
using namespace std;

//note an important barrier in run()

template <class VertexOLT, class AggregatorT = DummyAgg, class IndexT = char>//indexT is a per-worker index (e.g. InvIdx) ***
class WorkerOL {

	public:
		typedef vector<VertexOLT*> VertexContainer;
		typedef typename VertexContainer::iterator VertexIter;

		typedef typename VertexOLT::KeyType KeyT;
		typedef typename VertexOLT::MessageType MessageT;
		typedef typename VertexOLT::QueryType QueryT;
		typedef typename VertexOLT::HashType HashT;
		typedef typename VertexOLT::TaskT TaskT;
		typedef typename VertexOLT::QMap QMap;
		typedef typename QMap::iterator QMapIter;

		typedef typename AggregatorT::PartialType PartialT;
		typedef typename AggregatorT::FinalType FinalT;

		typedef MessageBufferOL<VertexOLT> MessageBufT;
		typedef typename MessageBufT::MessageContainerT MessageContainerT;
		typedef typename MessageBufT::Map Map;
		typedef typename MessageBufT::MapIter MapIter;

		HashT hash;
		VertexContainer vertexes;
		QMap queries;

		MessageBufT* message_buffer;
		Combiner<MessageT>* combiner;

		msg_queue_server* server;
		int nxt_qid;
		int glob_step;

		const char* output_folder;
		char outpath[50];
		char qfile[10];

		bool use_agg;
		bool save_tag;

		IndexT index;//per-worker index***
		bool use_index;

		WorkerOL(bool agg_used = false, bool save_at_last = false, bool idx_used = false)
		{
			init_workers();
			use_agg = agg_used;
			save_tag = save_at_last;
			use_index = idx_used;//per-worker index***
			glob_step = 1;
			global_vertexes = &vertexes;
			active_queries = &queries;
			message_buffer = new MessageBufT;
			global_message_buffer = message_buffer;
			combiner = NULL;
			global_combiner = NULL;
			if (_my_rank == MASTER_RANK) server=new msg_queue_server;
			nxt_qid=1;
		}

		void setCombiner(Combiner<MessageT>* cb)
		{
			combiner = cb;
			global_combiner = cb;
		}

		~WorkerOL()
		{
			for(int i = 0; i < vertexes.size(); i++) delete vertexes[i];
			delete message_buffer;
			if (_my_rank == MASTER_RANK) delete server;
			worker_finalize();
		}

		//==================================
		AggregatorT* get_aggregator()
		{
			TaskT& task=*(TaskT*)query_entry();
			return (AggregatorT*)(task.aggregator);
		}

		FinalT* get_agg()
		{
			TaskT& task=*(TaskT*)query_entry();
			return (FinalT*)(task.agg);
		}
		//==================================
		//sub-functions
		void sync_graph()
		{
			//ResetTimer(4);
			//set send buffer
			
			vector<VertexContainer> _loaded_parts(_num_workers);
			for (int i = 0; i < vertexes.size(); i++) {
				VertexOLT* v = vertexes[i];
				_loaded_parts[hash(v->id)].push_back(v);
			}
			
			//exchange vertices to add
			all_to_all(_loaded_parts);
			

			//delete sent vertices
			for (int i = 0; i < vertexes.size(); i++) {
				VertexOLT* v = vertexes[i];
				if (hash(v->id) != _my_rank)
					delete v;
			}
			vertexes.clear();
			
			//collect vertices to add
			for (int i = 0; i < _num_workers; i++) {
				vertexes.insert(vertexes.end(), _loaded_parts[i].begin(), _loaded_parts[i].end());
			}
			//StopTimer(4);
			//PrintTimer("Reduce Time",4);
		};

		//per-worker index***
		//index-construction UDFs
		virtual void load2Idx(char* line, IndexT& idx){}
		virtual void load2Idx(VertexOLT* v, int position, IndexT& idx){}
		virtual void idx_init(){}//if(use_index), the function will be called
		//functions for users to call in idx_init()
		void load_idx_from_file(char * idxpath)
		{
			hdfsFS fs = getHdfsFS();
			hdfsFile in = getRHandle(idxpath, fs);
			LineReader reader(fs, in);
			while (true) {
				reader.readLine();
				if (!reader.eof())
					load2Idx(reader.getLine(), index);
				else
					break;
			}
			hdfsCloseFile(fs, in);
			hdfsDisconnect(fs);
		}
		//----
		void load_idx_from_vertexes()
		{
			for(int i=0; i<vertexes.size(); i++)
			{
				VertexOLT* v = vertexes[i];
				load2Idx(v, i, index);
			}
		}
		//function for users to call in init(VertexContainer& vertex_vec)
		inline IndexT& idx(){ return index; }

		void compute_dump()
		{
			QMapIter qit = queries.begin();
			while(qit != queries.end())
			{
				TaskT& task=qit->second;
				task.check_termination();
				if(task.superstep!=-1){//compute
					//set query environment
					set_qid(qit->first);
					set_query_entry(&task);
					//process this query
					task.start_another_superstep();
					//- aggregator init (must call init() after superstep is increased)
					AggregatorT* aggregator;
					if(use_agg)
					{
						aggregator = get_aggregator();
						aggregator->init();
					}
					hash_set<int> active_set;
					task.move_active_vertices_to(active_set);
					for(hash_set<int>::iterator it=active_set.begin(); it!=active_set.end(); it++)
					{
						int pos=*it;
						VertexOLT* v=vertexes[pos];
						v->vertex_compute();
						if(use_agg) aggregator->stepPartial(v);
						if(v->is_active()) task.activate(pos);
					}
					//------
					++qit;
				}
				else//dump
				{
					//set query environment
					set_qid(qit->first);
					set_query_entry(&task);
					//dump query result
					strcpy(outpath, output_folder);
					sprintf(qfile, "/query%d", qit->first);
					strcat(outpath, qfile);
					dump_partition_and_free_states(outpath);
					int accessed=all_sum(task.created.size());//--------------------------------------------
					if(_my_rank==MASTER_RANK)
					{
						double time=task.get_runtime();
						double rate=((double)accessed)/get_vnum();
						//--------------------------------------------
						cout<<"Q"<<qit->first<<" dumped, response time: "<<time<<" seconds, vertex access rate: "<<rate<<endl;
						//--------------------------------------------
					}
					//free the query task
					if(use_agg)
					{
						delete (AggregatorT*)task.aggregator;
						delete (FinalT*)task.agg;
					}
					queries.erase(qit++);
				}
			}
			//-----------------------------------
			//aggregating
			if(use_agg)
			{
				for(qit = queries.begin(); qit != queries.end(); qit++)
				{
					//set query environment
					TaskT& task=qit->second;
					set_qid(qit->first);
					set_query_entry(&task);
					AggregatorT* aggregator = get_aggregator();
					//------
					FinalT* agg = get_agg();
					if (_my_rank != MASTER_RANK) { //send partialT to aggregator
						//gathering PartialT
						PartialT* part = aggregator->finishPartial();
						slaveGather(*part);
						//scattering FinalT
						slaveBcast(*agg);
					} else {
						//gathering PartialT
						vector<PartialT*> parts(_num_workers);
						masterGather(parts);
						for (int i = 0; i < _num_workers; i++) {
							if (i != MASTER_RANK) {
								PartialT* part = parts[i];
								aggregator->stepFinal(part);
								delete part;
							}
						}
						//scattering FinalT
						FinalT* final = aggregator->finishFinal();
						*agg = *final;//deep copy
						masterBcast(*agg);
					}
				}
			}
		}

		//agg_sync() to be implemented here
		//note that aggregator maintains partial states, and need to be associated with "Task" !!!

		//=============================================
		//UDF: how to initialize active (created)
		virtual void init(VertexContainer& vertex_vec)=0;
		//it can call get_vpos(), activate();

		//functions to be called in UDF init():
		int get_vpos(KeyT vertex_id)
		{
			//return -1 if the vertex with the specified id is not found
			return message_buffer->get_vpos(vertex_id);
		}

		//functions to be called in UDF init():
		QueryT* get_query()//called in compute()
		{
			TaskT& task=*(TaskT*)query_entry();
			return &task.query;
		}

		//WorkerOL/UDF_init() calls activate() to add a vertex to active_set
		void activate(int vertex_position)
		{
			TaskT& task=*(TaskT*)query_entry();
			task.activate(vertex_position);
		}

		//system call
		void task_init()
		{
			//init(vertexes);
			
			QueryT& queryContainer = *get_query();
			//cout << "queryContainer: " << endl;
			//cout << queryContainer.size() << endl;
			//for (int i = 0; i < queryContainer.size(); ++ i) cout << queryContainer[i] << endl;
			if (queryContainer[0] == 0 || queryContainer[0] == 1)
			{
				//get neighbors: 0, v, (t1,t2)
				int src = queryContainer[1];
				int pos = get_vpos(src);
				if (pos != -1) activate(pos);
			}
		}

		//------

		//UDF: how to dump a processed vertex
		virtual void dump(VertexOLT* vertex, BufferedWriter& writer)=0;
		//------
		//system call
		void dump_partition_and_free_states(char * outpath)
		{
			hdfsFS fs = getHdfsFS();
			BufferedWriter* writer = new BufferedWriter(outpath, fs, _my_rank);

			TaskT& task=*(TaskT*)query_entry();
			for (hash_set<int>::iterator it = task.created.begin(); it != task.created.end(); it++) {
				VertexOLT* v = vertexes[*it];
				writer->check();
				dump(v, *writer);
				v->free();
			}
			delete writer;
			hdfsDisconnect(fs);
		}

		//------

		string path2save;
		void set_file2save(string path)
		{
			path2save=path;
		}

		//UDF: how to dump a vertex's NQValue when the server is turned down
		virtual void save(VertexOLT* vertex, BufferedWriter& writer){};//default implemetation is doing nothing

		//system call
		void save_vertices()
		{
			hdfsFS fs = getHdfsFS();
			BufferedWriter* writer = new BufferedWriter(path2save.c_str(), fs, _my_rank);

			for (VertexIter it = vertexes.begin(); it != vertexes.end(); it++) {
				writer->check();
				save(*it, *writer);
			}
			delete writer;
			hdfsDisconnect(fs);
		}

		//=============================================

		//user-defined graphLoader ==============================
		virtual VertexOLT* toVertex(char* line) = 0; //this is what user specifies!!!!!!

		void load_vertex(VertexOLT* v)
		{ //called by load_graph
			vertexes.push_back(v);
		}

		void load_graph(const char* inpath)
		{
			hdfsFS fs = getHdfsFS();
			hdfsFile in = getRHandle(inpath, fs);
			LineReader reader(fs, in);
			while (true) {
				reader.readLine();
				if (!reader.eof())
					load_vertex(toVertex(reader.getLine()));
				else
					break;
			}
			hdfsCloseFile(fs, in);
			hdfsDisconnect(fs);
			//cout<<"Worker "<<_my_rank<<": \""<<inpath<<"\" loaded"<<endl;//DEBUG !!!!!!!!!!
		}
		//=======================================================

		//user-defined qyeryLoader ==============================
		virtual QueryT toQuery(char* line) = 0; //this is what user specifies!!!!!!

		struct qinfo
		{
			int qid;
			QueryT q;

			qinfo(){}

			qinfo(int qid, QueryT q)
			{
				this->qid=qid;
				this->q=q;
			}

			friend ibinstream& operator<<(ibinstream& m, const qinfo& v)
			{
				m << v.qid;
				m << v.q;
				return m;
			}

			friend obinstream& operator>>(obinstream& m, qinfo& v)
			{
				m >> v.qid;
				m >> v.q;
				return m;
			}
		};

		bool update_tasks()//return false to shut_down server
		{
			vector<qinfo> new_queries;
			if (_my_rank == MASTER_RANK)
			{
				long type=1;
				if(queries.size()>0)//has tasks running, do not wait till there are new queries
				{
					while(server->recv_msg(type))
					{
						char* msg=server->get_msg();
						cout<<"Q"<<nxt_qid<<": "<<msg<<endl;
						if(strcmp(msg, "server_exit")==0)
						{
							new_queries.clear();
							qinfo qentry(-1, QueryT());
							new_queries.push_back(qentry);
							masterBcast(new_queries);
							if(save_tag)
							{
								ResetTimer(WORKER_TIMER);
								save_vertices();
								StopTimer(WORKER_TIMER);
								PrintTimer("Vertices Saved, Time", WORKER_TIMER);
							}
							return false;
						}
						else
						{
							QueryT q=toQuery(msg);
							qinfo qentry(nxt_qid, q);
							new_queries.push_back(qentry);
							TaskT& task=queries[nxt_qid]=TaskT(q);
							if(use_agg)
							{
								task.aggregator=new AggregatorT;
								task.agg=new FinalT;
							}
							//create empty output_folder {
							strcpy(outpath, output_folder);
							sprintf(qfile, "/query%d", nxt_qid);
							strcat(outpath, qfile);
							dirCreate(outpath);
							//} create empty output_folder
							//set query environment
							set_qid(nxt_qid);
							set_query_entry(&task);
							//init active vertices
							task_init();
							//----
							nxt_qid++;
						}
					}
				}
				else//has no running task
				{
					while(server->recv_msg(type) == false);//busy waiting if server gets no query msg
					do{
						char* msg=server->get_msg();
						cout<<"Q"<<nxt_qid<<": "<<msg<<endl;
						if(strcmp(msg, "server_exit")==0)
						{
							new_queries.clear();
							qinfo qentry(-1, QueryT());
							new_queries.push_back(qentry);
							masterBcast(new_queries);
							if(save_tag)
							{
								ResetTimer(WORKER_TIMER);
								save_vertices();
								StopTimer(WORKER_TIMER);
								PrintTimer("Vertices Saved, Time", WORKER_TIMER);
							}
							return false;
						}
						else
						{
							QueryT q=toQuery(msg);
							qinfo qentry(nxt_qid, q);
							new_queries.push_back(qentry);
							TaskT& task=queries[nxt_qid]=TaskT(q);
							if(use_agg)
							{
								task.aggregator=new AggregatorT;
								task.agg=new FinalT;
							}
							//create empty output_folder {
							strcpy(outpath, output_folder);
							sprintf(qfile, "/query%d", nxt_qid);
							strcat(outpath, qfile);
							dirCreate(outpath);
							//} create empty output_folder
							//set query environment
							set_qid(nxt_qid);
							set_query_entry(&task);
							//init active vertices
							task_init();
							//----
							nxt_qid++;
						}
					}while(server->recv_msg(type));
				}
				masterBcast(new_queries);
			}
			else
			{
				slaveBcast(new_queries);
				if(new_queries.size()==1 && new_queries[0].qid==-1)
				{
					if(save_tag)
					{
						ResetTimer(WORKER_TIMER);
						save_vertices();
						StopTimer(WORKER_TIMER);
						PrintTimer("Vertices Saved, Time", WORKER_TIMER);
					}
					return false;
				}
				else
				{
					for(int i=0; i<new_queries.size(); i++)
					{
						qinfo& info=new_queries[i];
						TaskT& task=queries[info.qid]=TaskT(info.q);
						if(use_agg)
						{
							task.aggregator=new AggregatorT;
							task.agg=new FinalT;
						}
						//set query environment
						set_qid(info.qid);
						set_query_entry(&task);
						//init active vertices
						task_init();
					}
				}
			}
			return true;
		}

		//================================================

		// run the worker
		void run(const WorkerParams& params)
		{
			//check path + init
			if (_my_rank == MASTER_RANK) {
				if (dirCheck(params.input_path.c_str(), params.output_path.c_str(), _my_rank == MASTER_RANK, params.force_write) == -1)
					exit(-1);
				if(save_tag)
				{
					if (outDirCheck(path2save.c_str(), _my_rank == MASTER_RANK, params.force_write) == -1) exit(-1);
				}
			}
			output_folder = params.output_path.c_str();
			init_timers();

			//dispatch splits
			ResetTimer(WORKER_TIMER);
			vector<vector<string> >* arrangement;
			if (_my_rank == MASTER_RANK) {
				arrangement = params.native_dispatcher ? dispatchLocality(params.input_path.c_str()) : dispatchRan(params.input_path.c_str());
				masterScatter(*arrangement);
				vector<string>& assignedSplits = (*arrangement)[0];
				//reading assigned splits (map)
				for (vector<string>::iterator it = assignedSplits.begin();
					 it != assignedSplits.end(); it++)
					load_graph(it->c_str());
				delete arrangement;
			}
			else {
				vector<string> assignedSplits;
				slaveScatter(assignedSplits);
				//reading assigned splits (map)
				for (vector<string>::iterator it = assignedSplits.begin();
					 it != assignedSplits.end(); it++)
					load_graph(it->c_str());
			}

			//send vertices according to hash_id (reduce)
			
			sync_graph();
			message_buffer->init(vertexes);
			//barrier for data loading
			worker_barrier();
			StopTimer(WORKER_TIMER);
			PrintTimer("Load Time", WORKER_TIMER);

			//per-worker index***
			if(use_index)
			{
				init_timers();
				idx_init();
				worker_barrier();
				StopTimer(WORKER_TIMER);
				PrintTimer("Indexing Time", WORKER_TIMER);
			}
			//per-worker index***

			get_vnum() = all_sum(vertexes.size());//vertex addition/deletion not supported right now
			//=========================================================
			
			
			int prePhase = 4;
			if (_my_rank==MASTER_RANK) cout << "begin preCompute" << endl;
			for (int i = 1; i <= prePhase; ++ i)
			{
				//if (_my_rank==MASTER_RANK) cout << "phaseNum: " << i << endl;
				preCompute(i);
			}
			//sleep(100000);
			
			init_timers();
			while(update_tasks())
			{
				ResetTimer(WORKER_TIMER);
				ResetTimer(4);
				compute_dump();
				message_buffer->combine();
				message_buffer->sync_messages();
				//--------------------------------
				worker_barrier();//VERY IMPORTANT
				//in sync_messages(), master sends to other workers, and it should be before update_tasks(), where master also sends to other workers
				//--------------------------------
				StopTimer(WORKER_TIMER);
				if (_my_rank == MASTER_RANK){
					cout<<"---------------------------------------"<<endl;
					cout<<"Global clock tick #: "<<glob_step<<endl;
					glob_step++;
					cout<<"Time Elapsed: "<<get_timer(WORKER_TIMER)<<" seconds"<<endl;
				}
			}
		}

		//================================================
		
		void preCompute(int phaseNum)
		{
			//global_step_num = 0;
			
			int phaseSuperstep = 0;
			QueryT q;
			TaskT& task = queries[nxt_qid] = TaskT(q);
			//set query environment
			set_qid(nxt_qid);
			set_query_entry(&task);
			nxt_qid++;
			QMapIter qit = queries.begin();
			//activate
			for (int i = 0; i < vertexes.size(); ++ i)
			{
				activate(get_vpos(vertexes[i]->id));
			}
			while(1)
			{
				task.check_termination();
				if (_my_rank==MASTER_RANK) cout << "superstep: " << task.superstep << endl;
				if (task.superstep!=-1)
				{
					phaseSuperstep ++;
					//set_query_entry(&task);
					task.start_another_superstep();
					hash_set<int> active_set;
					task.move_active_vertices_to(active_set);
					for(hash_set<int>::iterator it=active_set.begin(); it!=active_set.end(); it++)
					{
						int pos=*it;
						VertexOLT* v=vertexes[pos];
						v->vertex_pre_compute(phaseNum);
						
						//if(v->is_active()) task.activate(pos);
					}
					
					//all the vertexes should be active in phase1 superstep1
					if (phaseNum == 1 && task.superstep==1)
					{
						for (int i = 0; i < vertexes.size(); ++ i) activate(get_vpos(vertexes[i]->id));
					}
				}
				else//dump
				{
					if (_my_rank==MASTER_RANK)
					{
						cout << "preCompute phaseNum: " << phaseNum << " done!" << endl;
						double time = task.get_runtime();
						cout << "time used: " << time << " seconds" << endl;
						cout << "total superstep: " << phaseSuperstep << endl;
						cout << "-------------------------------" << endl;
					}
					for (int i = 0; i < vertexes.size(); ++ i)
					{
						vertexes[i]->free();
					}
					queries.erase(qit++);
					break;
				}
				message_buffer->combine();
				message_buffer->sync_messages();
				//--------------------------------
				worker_barrier();
			}
			//sleep(10000000);
		}
		
		

		// run the worker
		/*
		void run(const MultiInputParams& params)
		{
			//check path + init
			if (_my_rank == MASTER_RANK) {
				if (dirCheck(params.input_paths, params.output_path.c_str(), _my_rank == MASTER_RANK, params.force_write) == -1)
					exit(-1);
				if(save_tag)
				{
					if (outDirCheck(path2save.c_str(), _my_rank == MASTER_RANK, params.force_write) == -1) exit(-1);
				}
			}
			output_folder = params.output_path.c_str();
			init_timers();

			//dispatch splits
			ResetTimer(WORKER_TIMER);
			vector<vector<string> >* arrangement;
			if (_my_rank == MASTER_RANK) {
				arrangement = params.native_dispatcher ? dispatchLocality(params.input_paths) : dispatchRan(params.input_paths);
				masterScatter(*arrangement);
				vector<string>& assignedSplits = (*arrangement)[0];
				//reading assigned splits (map)
				for (vector<string>::iterator it = assignedSplits.begin();
					 it != assignedSplits.end(); it++)
					load_graph(it->c_str());
				delete arrangement;
			} else {
				vector<string> assignedSplits;
				slaveScatter(assignedSplits);
				//reading assigned splits (map)
				for (vector<string>::iterator it = assignedSplits.begin();
					 it != assignedSplits.end(); it++)
					load_graph(it->c_str());
			}

			//send vertices according to hash_id (reduce)
			sync_graph();
			message_buffer->init(vertexes);
			//barrier for data loading
			worker_barrier();
			StopTimer(WORKER_TIMER);
			PrintTimer("Load Time", WORKER_TIMER);

			//per-worker index***
			if(use_index)
			{
				init_timers();
				idx_init();
				worker_barrier();
				StopTimer(WORKER_TIMER);
				PrintTimer("Indexing Time", WORKER_TIMER);
			}
			//per-worker index***

			get_vnum() = all_sum(vertexes.size());//vertex addition/deletion not supported right now
			//=========================================================
			
			
			preCompute();
			
			init_timers();
			while(update_tasks())
			{
				ResetTimer(WORKER_TIMER);
				ResetTimer(4);
				compute_dump();
				message_buffer->combine();
				message_buffer->sync_messages();
				//--------------------------------
				worker_barrier();//VERY IMPORTANT
				//in sync_messages(), master sends to other workers, and it should be before update_tasks(), where master also sends to other workers
				//--------------------------------
				StopTimer(WORKER_TIMER);
				if (_my_rank == MASTER_RANK){
					cout<<"---------------------------------------"<<endl;
					cout<<"Global clock tick #: "<<glob_step<<endl;
					glob_step++;
					cout<<"Time Elapsed: "<<get_timer(WORKER_TIMER)<<" seconds"<<endl;
				}
			}
		}
		*/
};

#endif
