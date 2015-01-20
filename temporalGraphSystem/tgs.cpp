#include <ol/pregel-ol-dev.h>
#include <utils/type.h>
#include <sstream>
#include <algorithm>
#include <set>
#include <map>
#include <assert.h>
//string in_path = "/yuzhen/toyP";
string in_path = "/yuzhen/sociopatterns";
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
    virtual void preCompute(MessageContainer& messages)
    {
    	if (superstep() == 1)
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
    	}
    	else
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
    		
    		//build index done
    		printTransformInfo();
    		
    	}
    	vote_to_halt();
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
	WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    temporalWorker worker;
    
    
    worker.run(param);
    return 0;
}
