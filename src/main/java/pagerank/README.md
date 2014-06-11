vi small_ext.txt

10      8
1       2
1       10
1       8
8       2
8       10
10      2
11      8
11      1
2       11

hadoop fs -put small_ext.txt /user/hpuser/


#prepare data

function load_data_map (InverseMapper)
input (PageN) -> (PageK)
begin
    output (PageK) -> (PageN)
end
 
function load_data_reduce
input (PageK) -> (PageA)
      (PageK) -> (PageB)
      ...
      (PageK) -> (PageN)
begin
    RankK := 0
    output (PageK : RankK) -> (PageA, PageB,..., PageN)
end


#update pagerank

function update_pagerank_map
input (PageN : RankN) -> (PageA, PageB, PageC ...)
begin
    Nn := the number of outlinks for PageN
    for each outlink PageK
        tmpRP = RankN/Nn
        output (PageK) -> (tmpRP)
    output (PageN) -> (PageA, PageB, PageC ...)
end
 
function update_pagerank_reduce
input (PageK) -> (tmpRP1)
      (PageK) -> (tmpRP2)
      ...
      (PageK) -> (tmpRPt)
      (PageK) -> (PageAk, PageBk, PageCk ...)
begin
    tmpRP := 0
    for each inlink tmpRPi
        tmpRP += tmpRPi
    RankK = tmpRP * d + (1 - d)    # d damping factor
    output (PageK, RankK) -> (PageAk, PageBk, PageCk ...)
end

