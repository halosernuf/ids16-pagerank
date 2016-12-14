# Pagerank of wikipedia

This project is to implement the PageRank to find the most important Wikipedia
pages on the provided the adjacency graph extracted from Wikipedia dataset using AWS Elastic
MapReduce(EMR).

## Calculate PageRank Score

Note that multipling a N on each side of the recurrence formula `PR(i)=(1-d)/N+d*SumAll(PR(j)/L(j))` won't harm its correctness. I initialize the PageRank of each page with 1 instead of 1/N.

###Step 1 Eliminate Redlink and initialize PageRank value

Using the same technique used in Lab5 to exclude redlink in the input graph. Additional column in each line after page name and initialized as 1.0

**Input < LongWritable, Text \>**
```
<Page>              <Links>
A                   Ampere  Encyclopædia_Britannica Alpha   EBCDIC  ASCII
A.C._Chievo_Verona  A.S._Roma   ACF_Fiorentina
A.D                 Anno_Domini     
```
**Output < Text, Text \>**
```
<Page>              <RankofPage>    <Links>
A                   1.0             EBCDIC  ASCII   Alpha   Encyclopædia_Britannica Ampere
A.C._Chievo_Verona  1.0             A.S._Roma   ACF_Fiorentina  
A.D                 1.0             Anno_Domini 
...
```

### Step 2 Calculate N

Using same method as wordCount to calculate the total number of pages in the input file

**MapOutput < Text, IntWritable \>**
```
<Marker>            <Count>
Total               1
Total               1
```
**Output < IntWritable, NullWritable \>**
```
<Count>
6273
...
```

Using `FileUtil.copyMerge` to merge the output folder into a single file.

### Step 3 Calculate PageRank*N

**MapInput < LongWritable, Text \>**
```
<Page>              <Rankofpage>    <links>
'Abd_al-Rahman_I    1.0             Baghdad    Damascus    Charlemagne Caliph  Ceuta   
...
```
**MapOutput < Text, Text \>**
```
<Links>             <RankofPage>    <TotalLinksofPage>  <Page>
Baghdad             1.0             5                   'Abd_al-Rahman_I
Damascus            1.0             5                   'Abd_al-Rahman_I
<Page>              <Marker>        <Links>
'Abd_al-Rahman_I    |               Baghdad    Damascus    Charlemagne Caliph  Ceuta   
...
```

Modified recurrence formula:  
`N*PR(i)=(1-d)+d*SumAll(PR(j)/L(j))`

New pagerank `N*PR(i)` is calculated for each page and wrote to the output. 
For Links with no inlinks, I wrote `<Page><marker><Links>` to the reduce process to make sure those links will not be excluded during recurrence process.  

**ReduceOutput < Text, Text \>**  
```
<Page>              <NewRankofPage> <Links>
'Abd_al-Rahman_I    0.14999998      Baghdad Damascus    Charlemagne Caliph  Ceuta
```

### Step 4 Devide N for each record

Because I preserved N at each recurrence step, at the final sort process, additional step of deviding N is needed to calculate the correct pagerank score.

**ReduceOutput < Text, DoubleWritable \>**  
```
<Page>              <NewRankofPage>
'Abd_al-Rahman_I    0.14999998/N=2.3912000637653437e-05      
```
