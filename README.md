# Using Dask

This repository will serve as a home for examples and refreshers of how to
properly utilize Dask for parallel processing of a variety of data collections.

## Dask

[Dask](http://dask.readthedocs.io/en/latest/) is a part of the [Blaze](http://blaze.pydata.org/) ecosystem and is defined as a "flexible parallel computing library for analytic computing".

Think of it as a way of transforming your large data collections and workflows into a collection of small pieces, each of which will be executed and stored only when necessary. In this way Dask is able to drastically reduce memory requirements and increase performance at the same time.

## Usage

When using Dask it is important to first identify what kind of task it is you want to complete. Is it a data transformation, aggregation, or collection job. Answering this question will guide you moving forward with how to guide Dask to handle the problem.

Transformation tasks are generally well documented and easy to implement while aggregation tasks can be significantly more difficult. I will include a section at the end about a specific aggregation task in order to provide an example of how to properly use Dask for a complicated case.

Let's saw we have a csv `my_csv.csv`.

```csv
,x,y
0,1,2.
1,2,4.
2,3,6.
3,4,8.
4,5,10.
```

First create a `dask.dataframe`.

```python
import dask.dataframe as dd

dask_df = dd.read_csv('my_csv.csv', blocksize=20)
```

**N.B.** The `read_csv` method can take any kwargs that are valid for `pandas.read_csv`. We also used `blocksize=20` in this case in order to force this small DataFrame to have multiple partitions.

For this example we would like to take this DataFrame, add a specified value to x, multiply the result by y and return the final value.

Transformations, as well as aggregations can be applied to the entire frame or just just one column. For applying the desired function to the entire DataFrame you can use [`apply`](http://dask.pydata.org/en/latest/dataframe-api.html#dask.dataframe.DataFrame.apply), [`applymap`](http://dask.pydata.org/en/latest/dataframe-api.html#dask.dataframe.DataFrame.applymap), or [`map_partitions`](http://dask.pydata.org/en/latest/dataframe-api.html#dask.dataframe.DataFrame.apply).

#### Apply

The `apply` method works much like `pandas.apply`, but it is worth noting that it can only be used with `axis=1` currently (i.e. apply over rows). Given the above example we could write a function `myadd` that we could apply over the entire DataFrame to get our result.

```python
def myadd(row, increment):
    return row[0] + increment * row[1]
```

To implement the `apply` method with our function we could do the following:

```python
result = dask_df.apply(myadd, axis=1, args=(2,)).compute(get=get)
print(result)
0   6.0
1   16.0
2   30.0
0   48.0
1   70.0
dtype: float64
```
**N.B.** Notice that the index is not contiguous. This is because the DataFrame is spilt between multiple partitions and each partition has its own index. To reset the index, just use `reset_index(drop=True)`. Alternatively, if we wanted to assign the result to a new column in dask_df we could do the following and the index would work itself out.

```python
dask_df['z'] = dask_df.apply(myadd, axis=1, args=(2,))
```

Great! We can apply a function over an entire DataFrame, using multiple columns in our calculation. But what if we just wanted to apply a function over each column of the whole DataFrame.

For example what if we just wanted to halve all of the values in each column. For that kind of situation you would use the `applymap` method.

#### Applymap

The `applymap` function allows you to apply a function across each column in a DataFrame individually. Essentially it separates the DataFrame into columns and maps the function over each column.

Let's use the example from above of halving all of the values. We can define a function `halve` and use the `applymap` method as follows:

```python
def halve(row):
    return row / 2

result = dask_df.applymap(lambda row: halve(row)).compute(get=get)
print(result)
    x     y
0   0.5   1.0
1   1.0   2.0
2   1.5   3.0
0   2.0   4.0
1   2.5   5.0
```

But you could do apply this same function using the `apply` method, so why use `applymap`? In some cases, especially as the total size of the DataFrame grows using `applymap` will confer a performance gain.

Now, since we have seen the behavior of these two different apply methods let's take a look at the `map_partitions` method.

#### Map_partitions

The `map_partitions` method allows us to apply a function to each partition independently. This can be useful for situations where you are working with large data sets or performing operations other than transformations (see [Aggregation Example](# Aggregation)).

In a simple transformation problem `map_partitions` is incredibly easy to implement. We just pass our function and Dask handles the rest.


```python
result = dask_df.map_partitions(halve).compute(get=get)
print(result)
    x     y
0   0.5   1.0
1   1.0   2.0
2   1.5   3.0
0   2.0   4.0
1   2.5   5.0
```

Again this method confers significant time savings as the DataFrame gets larger. In more complicated examples you can map over columns within the partition. For example say you only wanted to halve column `x`:

```python
def halve_map(partition):
    result = partition.x.map(halve)
    return result

result = dask_df.map_partitions(halve_map).compute(get=get)
print(result)
0   0.5
1   1.0
2   1.5
0   2.0
1   2.5
Name: x, dtype: float64
```

### Aggregation Example

Aggregation methods can be much more difficult to implement than transformations. I will provide on complex example to show how this can be handled and some of the difficulties involved.

#### Dataset

The primary data `trajs` for this example comes from a traffic modeling simulation and includes 24 fields, 4 of which contain up to 200 elements. This set is ~ 3.5 GB before loading and ~ 10 GB after loading and converting arrays. The pertinent field in this example is 'Nodes', one of the array fields. Nodes contains an array that contains all of the nodes that a given vehicle traversed on a given trip.

A secondary data source `tazs` comes from a network overview that contains 3 fields one of which is an array that can contain up to 7 elements. This set is ~ 150 MB after loading and converting arrays. The pertinent fields in this example are 'Node' and 'TazList'. The 'TazList' is an array containing the TAZ's that the given Node interacts with.

#### The Problem

For this example we want to be able to count the number of trips that are associated with each TAZ. For example lets say a trip contains the 'Nodes' array [1, 2, 4, 5]. We need to find the Taz's associated with each node and increment it by one. The logic is that the array [1, 2, 4, 5] actually contains 6 trips total and we need to account for each of these.

Let's say that the node associations are as follows:

```
Node  TazList
1     [1]
2     [1, 3]
4     [2, 3]
5     [2, 3, 4]
```
Given this trip what are the total number of nodes in each Taz that were visited?

For our question the answer is {1: 2, 2: 2, 3: 3, 4: 1}. We just retrieve the index from `tazs` for each node in our trip and increment a counter for each value we encounter.

Well we have only 4 lists of Taz's for the four nodes visited. Unfortunately trying to retrieve the index of a DataFrame that contains all of the nodes in a given trip for millions of records is inefficient. So we need a better way to get these values.

We know that we have < 300000 Nodes and that each nodes has at most 7 Taz's associated. So we can build a 300000 x 7 array of zeros and fill in the node values for the index along the first dimension that corresponds to a node number. So in our example Node 5 has TazList [2, 3, 4]. Lets call our array `node_tazs`. So `node_tazs[5] = [2, 3, 4, 0, 0, 0, 0]`. Now we have an efficient way of retrieving the tazs associated with a node.

Now we can start to work on our Dask function. We know that we only need to work with 1 column from `trajs`, but each row contains an array and we need to iterate over each element of these arrays. Conveniently we can just use each row array to index our `node_tazs` array and get back an n x 7 array where n is the number of nodes in the row array. So let's start building our function.

```python
def count_tazs(row, node_tazs):
    tazs = node_tazs[row]
```

Ok we are off. But now what should we do with this nx7 array. Iterate over each element. No! We only need the sum of the occurrences for each number so we can combine all of the values into bins. Thankfully numpy has a convenient method `bincount` to do just this. Now our function becomes:

```python
def count_tazs(row, node_tazs):
    tazs = node_tazs[row]
    counts = np.bincount(tazs.flatten())
```

Great! Now we need to add those values to a running counter. Well, using a Counter or Dictionary for this purpose would be inefficient as we are planning on running this in parallel. So lets just build another array of zeros equal to the number of Tazs present plus 1. We will call it `final_count`. Then we can just update final_count for each row.

```python
def count_tazs(row, node_tazs, final_count):
    tazs = node_tazs[row]
    counts = np.bincount(tazs.flatten())
    final_count[:len(bins)] += bins
    return final_count
```

Now theoretically we could use the `apply` method here, but if we do we end up getting an error from Dask as it is expecting a result with the same dimensions as the DataFrame and not the dimensions of final_value. So to get around this we will use the `map_partitions` method, and we will map the count_tazs method onto each partition. I believe this is a workaround for the error processing in Dask as we get back an array of shape 2108 * npartitions. That can then be organized back into the original shape with `np.reshape` and then summed to return the final array. One point worth noting is that we will need to set the `meta=` kwarg for `map_partitions` as Dask will not be able to interpret from the DataFrame what we want to return.

```python
def count_tazs(row, node_tazs, final_count):
    tazs = node_tazs[row]
    counts = np.bincount(tazs.flatten())
    final_count[:len(bins)] += bins


def map_count_tazs(partition, node_tazs, final_count):
    partition.Nodes.map(lambda x: count_tazs(x, node_tazs, final_count))
    return final_count

result = dask_df.map_partitions(
    map_count_tazs,
    node_tazs,
    final_counts,
    meta='object').compute(get=get)
```  
