//s-t shortest path on unweighted graph, by single direction BFS

#include "ol/pregel-ol-dev.h"
#include "utils/type.h"

string in_path = "/pullgel/amazon";
string out_path = "/ol_out";
bool use_combiner = true;

//input line format: vertexID \t numOfNeighbors neighbor1 neighbor2 ...
//edge lengths are assumed to be 1

//output line format: v \t shortest_path_length

//--------------------------------------------------

//Step 1: define static field of vertex: adj-list
struct BFSValue {
    vector<int> nbs;
};

ibinstream& operator<<(ibinstream& m, const BFSValue& v)
{
    m << v.nbs;
    return m;
}

obinstream& operator>>(obinstream& m, BFSValue& v)
{
    m >> v.nbs;
    return m;
}

//--------------------------------------------------

//Step 2: define query type: here, it is intpair (src, dst)
//Step 3: define query-specific vertex state: here, it is hop_to_src (int)
//Step 4: define msg type: here, it is just a signal, can be anything like char

//--------------------------------------------------
//Step 5: define vertex class

char dummy_msg = 0;

class BFSVertex : public VertexOL<VertexID, int, BFSValue, char, intpair> {
public:
    //Step 5.1: define UDF1: query -> vertex's query-specific init state
    virtual int init_value(intpair& query)
    {
        if (id == query.v1)
            return 0;
        else
            return INT_MAX;
    }

    //Step 5.2: vertex.compute
    virtual void compute(MessageContainer& messages)
    {
        if (superstep() == 1) {
            //broadcast
            vector<int>& nbs = nqvalue().nbs;
            for (int i = 0; i < nbs.size(); i++)
                send_message(nbs[i], dummy_msg);
        } else if (qvalue() == INT_MAX) //not reached before
        {
            qvalue() = superstep() - 1; //step i marks all vertices (i-1) hopsv away from src
            if (id == get_query().v2)
                forceTerminate(); //dst reached
            else {
                //broadcast
                vector<int>& nbs = nqvalue().nbs;
                for (int i = 0; i < nbs.size(); i++)
                    send_message(nbs[i], dummy_msg);
            }
        }
        vote_to_halt();
    }
};

//--------------------------------------------------
//Step 6: define worker class
class BFSWorkerOL : public WorkerOL<BFSVertex> {
public:
    //Step 6.1: UDF: line -> vertex
    virtual BFSVertex* toVertex(char* line)
    {
        char* pch;
        pch = strtok(line, "\t");
        BFSVertex* v = new BFSVertex;
        int id = atoi(pch);
        v->id = id;
        strtok(NULL, " "); //skip neighbor_number
        while (pch = strtok(NULL, " ")) {
            int nb = atoi(pch);
            v->nqvalue().nbs.push_back(nb);
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

    //Step 6.4: UDF: task_dump
    virtual void dump(BFSVertex* vertex, BufferedWriter& writer)
    {
        if (vertex->id != get_query().v2)
            return;
        char buf[50];
        sprintf(buf, "%d\t%d\n", vertex->id, vertex->qvalue());
        writer.write(buf);
    }
};

class BFSCombiner : public Combiner<char> {
public:
    virtual void combine(char& old, const char& new_msg)
    {
    }
};

int main_bfs(int argc, char* argv[])
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    BFSWorkerOL worker;
    BFSCombiner combiner;
    if (use_combiner)
        worker.setCombiner(&combiner);
    worker.run(param);
    return 0;
}
