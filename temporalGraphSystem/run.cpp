#include <ol/pregel-ol-dev.h>
#include <utils/type.h>
#include <sstream>
#include <algorithm>
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
ibinstream & operator<<(ibinstream & m, const pair<int,int> p) 
{m << p.first << p.second; return m;}
obinstream& operator>>(obinstream& m, vertexValue& v)
{
    m >> v.tw;
    m >> v.neighbors;
    return m;
}
obinstream & operator>>(obinstream & m, pair<int,int>& p)
{m >> p.first >> p.second; return m;}


class temporalVertex : public VertexOL<VertexID, int, vertexValue, int, intpair>{
public:
	//Step 5.1: define UDF1: query -> vertex's query-specific init state
    virtual int init_value(intpair& query)
    {
        if (id == query.v1)
            return 0;
        else
            return INT_MAX;
    }
    virtual void compute(MessageContainer& messages)
    {
    	return;
    }
   
    virtual void preCompute(MessageContainer& messages)
    {
    	return;
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
    virtual intpair toQuery(char* line)
    {
        char* pch;
        pch = strtok(line, " ");
        int src = atoi(pch);
        pch = strtok(NULL, " ");
        int dst = atoi(pch);
        return intpair(src, dst);
    }

    //Step 6.3: UDF: vertex init
    virtual void init(VertexContainer& vertex_vec)
    {
        int src = get_query().v1;
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
