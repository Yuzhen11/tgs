#include <ol/pregel-ol-dev.h>
#include <utils/type.h>
#include <sstream>
#include <algorithm>
#include <set>
#include <map>
#include <queue>
#include <assert.h>
string in_path = "/yuzhen/toyP";
//string in_path = "/yuzhen/sociopatterns";
//string in_path = "/yuzhen/editP";
string out_path = "/yuzhen/output";
bool use_combiner = true;

struct vertexValue {
    vector<vector<pair<int,int> > > tw; //(t,w)
    vector<int> neighbors;
   
};

ibinstream& operator<<(ibinstream& m, const vertexValue& v)
{
    m << v.tw;
    m << v.neighbors;
    return m;
}
ibinstream & operator<<(ibinstream & m, const pair<int,int>& p) 
{m << p.first << p.second; return m;}
obinstream& operator>>(obinstream& m, vertexValue& v)
{
    m >> v.tw;
    m >> v.neighbors;
    return m;
}
obinstream & operator>>(obinstream & m, pair<int,int>& p)
{m >> p.first >> p.second; return m;}

struct vw
{
	int v, w;
};

bool cmp2(const pair<int,int>& p1, const pair<int,int>& p2)
{
	if (p1.first == p2.first) return p1.second > p2.second;
	return p1.first < p2.first;
}

//idType, qvalueType, nqvalueType, messageType, queryType
class temporalVertex : public VertexOL<VertexID, int, vertexValue, vector<int>, vector<int> >{
public:
	
	std::map<int,int> mOut;
	int countOut;
	vector<int> Vout; //store time
	vector<vector<vw> > VoutNeighbors; //store neighbors
	
	std::map<int,int> mIn;
	int countIn;
	vector<int> Vin;
	vector<vector<vw> > VinNeighbors;
	
	
	vector<int> toOut;
	vector<int> toIn;
	//topological level
	vector<int> topologicalLevelIn;
	vector<int> topologicalLevelOut;
	vector<int> inDegreeIn;
	vector<int> inDegreeOut;
	vector<int> countinDegreeIn;
	vector<int> countinDegreeOut;
	
	//topChain
	int labelSize;
	vector<vector<pair<int,int> > > LinIn;
	vector<vector<pair<int,int> > > LinOut;
	vector<vector<pair<int,int> > > LoutIn;
	vector<vector<pair<int,int> > > LoutOut;
	
	vector<vector<pair<int,int> > > LtinIn;
	vector<vector<pair<int,int> > > LtinOut;
	vector<vector<pair<int,int> > > LtoutIn;
	vector<vector<pair<int,int> > > LtoutOut;
	
	vector<vector<pair<int,int> > > LcinIn;
	vector<vector<pair<int,int> > > LcinOut;
	vector<vector<pair<int,int> > > LcoutIn;
	vector<vector<pair<int,int> > > LcoutOut;
	

	//Step 5.1: define UDF1: query -> vertex's query-specific init state
    virtual int init_value(vector<int>& query) //QValueT init_value(QueryT& query
    {
    	/*
        if (id == query.v1)
            return 0;
        else
            return INT_MAX;
        */
        return -1;
    }
    virtual void compute(MessageContainer& messages)
    {
    	return;
    }
    
    
    
    void mergeOut(vector<pair<int,int> >& final, vector<pair<int,int> >& v1, vector<pair<int,int> >& v2, vector<pair<int,int> >& store, k)
    {
    	//merge v1, v2, store in tmp, merge tmp, final store in final, keep change in store
    	vector<pair<int,int> > tmp;
    	int p1,p2;
    	p1 = p2 = 0;
    	while(p1 < v1.size() || p2 < v2.size())
    	{
    		if (p1 == v1.size() || v2[p2].first < v1[p1].first) {tmp.push_back(v2[p2]); p2++; }
    		else if (p2 == v2.size() || v1[p1].first < v2[p2].first) {tmp.push_back(v1[p1]); p1++; }
    		else {tmp.push_back(min(v1[p1],v2[p2])); p1++; p2++; }
    		
    		if (tmp.size() == k) break;
    	}
    	
    	p1 = p2 = 0;
    	vector<pair<int,int> > final2;
    	while(p1 < final.size() || p2 < tmp.size())
    	{
    		if (p1 == final.size() || tmp[p2].first < final[p1].first) {final2.push_back(tmp[p2]); store.push_back(tmp[p2]); p2++;}
    		else if (p2 == tmp.size() || final[p1].first < tmp[p2].first) {final2.push_back(final[p1]); p1++; }
    		else if (final[p1].second <= tmp[p2].second) {final2.push_back(final[p1]); p1++; p2++;}
    		else {final2.push_back(tmp[p2]); store.push_back(tmp[p2]); p1++; p2++;}
	   		if (final2.size() == k) break;
    	}
    	final = final2;
    }
    void mergeIn(vector<pair<int,int> >& final, vector<pair<int,int> >& v1, vector<pair<int,int> >& v2, vector<pair<int,int> >& store, k)
    {
    	//merge v1, v2, store in tmp, merge tmp, final store in final, keep change in store
    	vector<pair<int,int> > tmp;
    	int p1,p2;
    	p1 = p2 = 0;
    	while(p1 < v1.size() || p2 < v2.size())
    	{
    		if (p1 == v1.size() || v2[p2].first < v1[p1].first) {tmp.push_back(v2[p2]); p2++; }
    		else if (p2 == v2.size() || v1[p1].first < v2[p2].first) {tmp.push_back(v1[p1]); p1++; }
    		else {tmp.push_back(max(v1[p1],v2[p2])); p1++; p2++; } //max
    		
    		if (tmp.size() == k) break;
    	}
    	
    	p1 = p2 = 0;
    	vector<pair<int,int> > final2;
    	while(p1 < final.size() || p2 < tmp.size())
    	{
    		if (p1 == final.size() || tmp[p2].first < final[p1].first) {final2.push_back(tmp[p2]); store.push_back(tmp[p2]); p2++;}
    		else if (p2 == tmp.size() || final[p1].first < tmp[p2].first) {final2.push_back(final[p1]); p1++; }
    		else if (final[p1].second >= tmp[p2].second) {final2.push_back(final[p1]); p1++; p2++;} //>=
    		else {final2.push_back(tmp[p2]); store.push_back(tmp[p2]); p1++; p2++;}
    		if (final2.size() == k) break;
    	}
    	final = final2;
    }
    void printTransformInfo()
    {
    	//print
		/*
		cout << id << endl;
		cout << "in" << endl;
		for (int i = 0; i < Vin.size(); ++ i)
		{
			cout << "t:" << Vin[i] << " ";
			for (int j = 0; j < VinNeighbors[i].size(); ++ j) cout << VinNeighbors[i][j].v << " " << VinNeighbors[i][j].w << " ";
			cout << "| "; 
		}
		cout << endl;
		
		cout << "out" << endl;
		for (int i = 0; i < Vout.size(); ++ i)
		{
			cout << "t:" << Vout[i] << " ";
			for (int j = 0; j < VoutNeighbors[i].size(); ++ j) cout << VoutNeighbors[i][j].v << " " << VinNeighbors[i][j].w << " ";
			cout << "| ";
		}
		cout << endl;
		*/
		
		int tmp;
		for (int i = 0; i < Vin.size(); ++ i)
		{
			tmp = Vin[i];
			for (int j = 0; j < VinNeighbors[i].size(); ++ j) {tmp = VinNeighbors[i][j].v; tmp = VinNeighbors[i][j].w;}
			
		}
		
		
		
		for (int i = 0; i < Vout.size(); ++ i)
		{
			tmp = Vout[i];
			for (int j = 0; j < VoutNeighbors[i].size(); ++ j) {tmp = VoutNeighbors[i][j].v; tmp = VinNeighbors[i][j].w;}
		}
    }
    virtual void preCompute(MessageContainer& messages, int phaseNum)
    {
    	if (phaseNum == 1)
    	{
			if (superstep() == 1) //build neighbors
			{
				//build out-neighbors
				countOut = 0;
				for (int i = 0; i < nqvalue().neighbors.size(); ++ i)
				{
					for (int j = 0; j < nqvalue().tw[i].size(); ++ j)
					{
						if (mOut.count(nqvalue().tw[i][j].first) == 0) 
						{
							mOut[nqvalue().tw[i][j].first] = 1;
						}
					}
				}
				for (std::map<int,int>::iterator it = mOut.begin(); it != mOut.end(); it ++)
				{
					it->second = countOut ++;
					Vout.push_back(it->first);
				}
				assert(countOut == Vout.size());
				VoutNeighbors.resize(countOut);
				for (int i = 0; i < nqvalue().neighbors.size(); ++ i)
				{
					for (int j = 0; j < nqvalue().tw[i].size(); ++ j)
					{
						VoutNeighbors[mOut[nqvalue().tw[i][j].first] ].push_back(vw{nqvalue().neighbors[i], nqvalue().tw[i][j].second});		
					}
				}
				//out-neighbors built done
				
				
				
				//send messages
				//message: id, t+w, w
				vector<int> send(3);
				for (int i = 0; i < nqvalue().neighbors.size(); ++ i)
				{
					//send_message(nqvalue().neighbors[i], id);
					send[0]=id;
					for (int j = 0; j < nqvalue().tw[i].size(); ++ j)
					{
						send[1]=nqvalue().tw[i][j].first+nqvalue().tw[i][j].second;
						send[2]=nqvalue().tw[i][j].second;
						send_message(nqvalue().neighbors[i], send);
					}
				}
				//send done
				
				//release memory
				for (int i = 0; i < nqvalue().neighbors.size(); ++ i) vector<pair<int,int> >().swap(nqvalue().tw[i]);
				vector<int>().swap(nqvalue().neighbors);
				vector<vector<pair<int,int> > >().swap(nqvalue().tw);
			}
			else if (superstep() == 2)
			{
				
				//build in-neighbors
				countIn = 0;
				for (int i = 0; i < messages.size(); ++ i)
				{
					vector<int>& edge = messages[i];
					if (mIn.count(edge[1]) == 0) mIn[edge[1]] = 1;
				}
				for (std::map<int,int>::iterator it = mIn.begin();it!=mIn.end();it++)
				{
					it->second = countIn ++;
					Vin.push_back(it->first);
				}
				assert(countIn == Vin.size());
				VinNeighbors.resize(countIn);
				for (int i = 0; i < messages.size(); ++ i)
				{
					vector<int>& edge = messages[i];
					VinNeighbors[mIn[edge[1]] ].push_back(vw{edge[0],edge[2]});
				}
				//in-neighbors built done
				
				
				//build toIn, toOut
				toIn.resize(Vout.size(),-1);
				toOut.resize(Vin.size(),-1);
				
				int lastOut = -1;
				for (int i = Vin.size()-1; i >= 0; --i)
				{
					std::map<int,int>::iterator it;
					it = mOut.lower_bound(Vin[i]);
					if (it!=mOut.end() && it->second != lastOut)
					{
						lastOut = it->second;
						toOut[i] = it->second;
						toIn[it->second] = i;
					}
				}
				
				//cout << "id:" << id << endl;
				//initialize inDegree 
				inDegreeIn.resize(Vin.size());
				inDegreeOut.resize(Vout.size());
				for (int i = 0; i < Vin.size(); ++ i)
				{
					inDegreeIn[i] = VinNeighbors[i].size()+(i==0?0:1);
					//cout << inDegreeIn[i] << endl;
				}
				for (int i = 0; i < Vout.size(); ++ i)
				{
					inDegreeOut[i] = (toIn[i]==-1?0:1) + (i==0?0:1);
					//cout << inDegreeOut[i] << endl;
				}
				
				//build neighbors done
				printTransformInfo();
				
				
				vote_to_halt();
			}
			
		}
		else if (phaseNum == 2) //build topological level
		{
			//cout << "phaseNum: " << phaseNum << endl;
			if (superstep() == 1)
			{
				
				countinDegreeIn.resize(Vin.size(), 0);
				countinDegreeOut.resize(Vout.size(), 0);
				topologicalLevelIn.resize(Vin.size(), -1);
				topologicalLevelOut.resize(Vout.size(), -1);
				
				vector<int> send(2); //message: level, arrivalTime, 
				if (Vout.size() > 0 && inDegreeOut[0] == 0)
				{
					topologicalLevelOut[0] = 0;
					send[0] = 1;
					for (int j = 0; j < VoutNeighbors[0].size(); ++ j)
					{
						send[1] = Vout[0] + VoutNeighbors[0][j].w;
						send_message(VoutNeighbors[0][j].v, send);
					}
					
					//check other Vout
					int nxt_id = 1;
					while(nxt_id < Vout.size())
					{
						countinDegreeOut[nxt_id] ++;
						if (countinDegreeOut[nxt_id] == inDegreeOut[nxt_id])
						{
							topologicalLevelOut[nxt_id] = topologicalLevelOut[nxt_id-1] + 1;
							send[0] = topologicalLevelOut[nxt_id]+1;
							for (int j = 0; j < VoutNeighbors[nxt_id].size(); ++ j)
							{
								send[1] = Vout[nxt_id] + VoutNeighbors[nxt_id][j].w;
								send_message(VoutNeighbors[nxt_id][j].v, send);
							}
							nxt_id++;
						}
						else
						{
							break;
						}
					}
				}
			}
			else
			{
				queue<pair<int,bool> > q; //id, 0/1,    0 for Vin, 1 for Vout
				for (int i = 0; i < messages.size(); ++ i)
				{
					vector<int>& inMsg = messages[i];
					int in_index = mIn[inMsg[1]];
					countinDegreeIn[in_index] ++;
					
					topologicalLevelIn[in_index] = max(topologicalLevelIn[in_index],inMsg[0]);
					if (countinDegreeIn[in_index] == inDegreeIn[in_index])
					{
						
						q.push(make_pair(in_index, 0));
					}
				}
				while(!q.empty())
				{
					pair<int,bool> cur = q.front(); q.pop();
					if (cur.second == 0) //Vin
					{
						if (cur.first != Vin.size()-1)
						{
							int tmp = cur.first+1;
							countinDegreeIn[tmp] ++;
							if (countinDegreeIn[tmp] == inDegreeIn[tmp])
							{
								topologicalLevelIn[tmp] = max(topologicalLevelIn[tmp],topologicalLevelIn[cur.first]+1);
								q.push(make_pair(tmp, 0));
							}
						}
						int tmp = toOut[cur.first];
						if (tmp != -1)
						{
							countinDegreeOut[tmp] ++;
							if (countinDegreeOut[tmp] == inDegreeOut[tmp])
							{
								topologicalLevelOut[tmp] = max(topologicalLevelOut[tmp],topologicalLevelIn[cur.first]+1);
								q.push(make_pair(tmp,1));
							}
						}
					}
					else if (cur.second == 1) //Vout
					{
						if (cur.first != Vout.size()-1)
						{
							int tmp = cur.first+1;
							countinDegreeOut[tmp]++;
							if (countinDegreeOut[tmp] == inDegreeOut[tmp])
							{
								topologicalLevelOut[tmp] = max(topologicalLevelOut[tmp],topologicalLevelOut[cur.first]+1);
								q.push(make_pair(tmp,1));
							}
						}
						vector<int> send(2); //level, arrivalTime
						send[0] = topologicalLevelOut[cur.first]+1;
						for (int i = 0; i < VoutNeighbors[cur.first].size(); ++ i)
						{
							send[1] = Vout[cur.first]+VoutNeighbors[cur.first][i].w;
							send_message(VoutNeighbors[cur.first][i].v, send);
						}
					}
				}
			}
			vote_to_halt();
		}
		else if (phaseNum == 3) //build topChain
		{
			
			if (superstep() == 1)
			{
				//release memory
				vector<int>().swap(countinDegreeIn);
				vector<int>().swap(countinDegreeOut);
				vector<int>().swap(inDegreeIn);
				vector<int>().swap(inDegreeOut);
			
			
				//cout << "phaseNum: " << phaseNum << endl;
				/*
				cout << "id: " << id << endl;
				for (int i = 0; i < Vin.size(); ++ i)
				{
					cout << topologicalLevelIn[i] << endl;
				}
				for (int i = 0; i < Vout.size(); ++ i)
				{
					cout << topologicalLevelOut[i] << endl;
				}
				*/
			
				labelSize = 2;
				LinIn.resize(Vin.size()); //inlabel for Vin
				LoutIn.resize(Vin.size()); //outlabel for Vin
				LinOut.resize(Vout.size());
				LoutOut.resize(Vout.size());
				
				LtinIn.resize(Vin.size());
				LtoutIn.resize(Vin.size());
				LtinOut.resize(Vout.size());
				LtoutOut.resize(Vout.size());
				
				LcinIn.resize(Vin.size());
				LcoutIn.resize(Vin.size());
				LcinOut.resize(Vout.size());
				LcoutOut.resize(Vout.size());
				
				vector<int> send(3);
				for (int i = 0; i < Vin.size(); ++ i)
				{
					LinIn[i].push_back(make_pair(id, Vin[i]));
					LoutIn[i].push_back(make_pair(id, Vin[i]));
					
					send[1] = LoutIn[i][0].first;
					send[2] = LoutIn[i][0].second;
					for (int j = 0; j < VinNeighbors[i].size(); ++ j)
					{
						send[0] = Vin[i]-VinNeighbors[i][j].w;	//time
						send_message(VinNeighbors[i][j].v, send);
					}
				}
				for (int i = 0; i < Vout.size(); ++ i)
				{
					LinOut[i].push_back(make_pair(id, Vout[i]));
					LoutOut[i].push_back(make_pair(id, Vout[i]));
					
					send[1] = LinOut[i][0].first;
					send[2] = LinOut[i][0].second;
					for (int j = 0; j < VoutNeighbors[i].size(); ++ j)
					{
						send[0] = -(Vout[i]+VoutNeighbors[i][j].w); //- means in-label
						send_message(VoutNeighbors[i][j].v, send);
					}
				}
			}
			else //message: t(in-, out+), <int,int>
			{
				//clear Lt, Lc
				for (int i = 0; i < Vin.size(); ++ i)
				{
					LtinIn[i].clear();
					LtoutIn[i].clear();
					LcinIn[i].clear();
					LcoutIn[i].clear();
				}
				for (int i = 0; i < Vout.size(); ++ i)
				{
					LtinOut[i].clear();
					LtoutOut[i].clear();
					LcinOut[i].clear();
					LcoutOut[i].clear();
				}
				
				for (int i = 0; i < messages.size(); ++ i) //receive messages and save in Lt
				{
					vector<int>& msg = messages[i];
					if (msg[0] >= 0) //out-label
					{
						LtoutOut[msg[0]].push_back(make_pair(msg[1], msg[2]));
					}
					else if (msg[0] < 0) //in-label
					{
						LtinIn[-msg[0]].push_back(make_pair(msg[1], msg[2]));
					}
				}
				for (int i = 0; i < Vin.size(); ++ i) sort(LtinIn[i].begin(), LtinIn[i].end(), cmp2);
				for (int i = 0; i < Vout.size(); ++ i) sort(LtoutOut[i].begin(), LtoutOut[i].end());
				
				//reverse topological order
				int p1 = Vin.size()-1;
				int p2 = Vout.size()-1;
				while(p1>=0||p2>=0)
				{
					if (p1 < 0 || Vin[p1] <= Vout[p2])
					{
						//visit Vout[p2], then p2--
						//merge LoutOut[p2] with LtoutOut[p2] and potentially LcoutOut[p2+1]
						if (p2 == Vout.size()-1)
						{
							vector<pair<int,int> > tmp;
							mergeOut(LoutOut[p2], tmp, LtoutOut[p2], LcoutOut[p2], labelSize);
						}
						else mergeOut(LoutOut[p2], LcoutOut[p2+1], LtoutOut[p2], LcoutOut[p2], labelSize);
						
						p2--;
					}
					else if (p2 < 0 || Vin[p1] > Vout[p2])
					{
						//visit Vin[p1], then p1 --
						if (p1 == Vin.size()-1)
						{
							vector<pair<int,int> > tmp;//,tmp2;
							if (toOut[p1] == -1) ;//mergeOut(LoutIn[p1], tmp, tmp2, LcoutIn[p1], labelSize);
							else mergeOut(LoutIn[p1], tmp, lcoutOut[toOut[p1]], LcoutIn[p1], labelSize);
						}
						else
						{
							if (toOut[p1] == -1 )
							{
								vector<pair<int,int> > tmp;
								mergeOut(LoutIn[p1], LcoutIn[p1+1], tmp2, LcoutIn[p1], labelSize);
							}
							else mergeOut(LoutIn[p1], LcoutIn[p1+1], LcoutOut[toOut[p1]], LcoutIn[p1], labelSize);
						}
						p1--;
					}
				}
				//topological order
				p1 = p2 = 0;
				while(p1 < Vin.size() || p2 < Vout.size())
				{
					if (p1 == Vin.size() || Vin[p1] > Vout[p2])
					{
						//visit Vout[p2]
						if (p2 == 0)
						{
							vector<pair<int,int> > tmp;//,tmp2;
							if (toIn[p2] == -1) ; //mergeIn(LinOut[p2], tmp, tmp2, LcinOut[p2], labelSize);
							else mergeIn(LinOut[p2], tmp, LcinIn[toIn[p2]], LcinOut[p2], labelSize);
						}
						else 
						{
							if (toIn[p2] == -1) 
							{	
								vector<pair<int,int> > tmp;
								mergeIn(LinOut[p2], LcinOut[p2-1], tmp, LcinOut[p2], labelSize);
							}
							else mergeIn(LinOut[p2], LcinOut[p2-1], LcinIn[toIn[p2]], LcinOut[p2], labelSize);
						}
						p2++;
					}
					else if (p2 == Vout.size() || Vin[p1] <= Vin[p2])
					{
						if (p1 == 0)
						{	
							vector<pair<int,int> > tmp;
							mergeIn(LinIn[p1], tmp, LtinIn[p1], LcinIn[p1], labelSize);
						}
						else mergeIn(LinIn[p1], LcinIn[p1-1], LtinIn[p1], LcinIn[p1], labelSize);
						p1++;
					}
				}
				//send messages
				vector<int> send(3);
				for (int i = 0; i < Vin.size(); ++ i)
				{
					if (VcoutIn[i].size() > 0)
					{
						for (int j = 0; j < VinNeighbors[i].size(); ++ j)
						{
							send[0] = Vin[i]-VinNeighbors[i][j].w;
							for (int k = 0; k < VcoutIn[i].size(); ++ k)
							{
								send[1] = VcoutIn[i][k].first;
								send[2] = VcoutIn[i][k].second;
								send_message(VinNeighbors[i][j].v, send);
							}
						}
					}
				}
				for (int i = 0; i < Vout.size(); ++ i)
				{
					if (VcinOut.size() > 0)
					{
						for (int j = 0; j < VoutNeighbors[i].size(); ++ j)
						{
							send[0] = -(Vout[i]+VoutNeighbors[i][j].w);
							for (int k = 0; k < VcinOut[i].size(); ++ k)
							{
								send[1] = VcinOut[i][k].first;
								send[2] = VcinOut[i][k].second;
								send_message(VoutNeighbors[i][j].v, send);
							}
						}
					}
				}
			}
		}
    }
    
    
};

//define worker class
class temporalWorker : public WorkerOL<temporalVertex>{
public:
	virtual temporalVertex* toVertex(char* line)
	{
		temporalVertex* v = new temporalVertex;
		istringstream ssin(line);
		int from, num_of_neighbors, to, num_of_t, w, t;
        ssin >> from >> num_of_neighbors;
        v->id = from;
        v->nqvalue().neighbors.resize(num_of_neighbors);
        v->nqvalue().tw.resize(num_of_neighbors);
        for (int i = 0; i < num_of_neighbors; ++ i)
        {
        	ssin >> to >> num_of_t >> w;
        	v->nqvalue().neighbors[i] = to;
        	for (int j = 0; j < num_of_t; ++ j)
        	{
        		ssin >> t;
        		v->nqvalue().tw[i].push_back(make_pair(t,w));
        	}
        }
        return v;
	}
	//Step 6.2: UDF: query string -> query (src_id)
    virtual vector<int> toQuery(char* line)
    {
        //query type is vector<int> 
        vector<int> ret;
        istringstream ssin(line);
        int tmp;
        while(ssin >> tmp) ret.push_back(tmp);
        
        return ret;
    }

    //Step 6.3: UDF: vertex init
    virtual void init(VertexContainer& vertex_vec)    
    {
	    //int src = get_query().v1;
        //vector <int> tt = get_query();
        int src;
        int pos = get_vpos(src);
        if (pos != -1)
            activate(pos);
    }
	virtual void dump(temporalVertex* vertex, BufferedWriter& writer)
	{
		
	}
};

int main(int argc, char* argv[])
{
	in_path = argv[1];
	
	WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    temporalWorker worker;
    
    
    worker.run(param);
    return 0;
}
