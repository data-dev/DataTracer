# Benchmarking DataTracer
This directory contains code for benchmarking the performance of `DataTracer` 
on user-supplied datasets. The datasets for benchmarking can be found in the
`s3://tracer-data` bucket.

## Primary Key
Primary key detection is evaluated by:

 - Accuracy. The percent of tables where the primary key was correctly identified.
 - Inference time. The amount of time to infer the primary key for all tables.

We will use leave-one-out validation and report the test performance on each dataset
in the S3 bucket.

## Foreign Key
Foreign key detection is evaluated by:

 - F1.
 - Recall.
 - Precision.
 - Inference time.

Note that this assumes that the foreign key primitive returns a set of foreign keys;
in other words, for models that return a score for each candidate foreign key, this
assumes that thresholding is done.

We will use leave-one-out validation and report the test performance on each dataset
in the S3 bucket.

## Foreign Key (No Thesholding)
Foreign key detection (no thresholding) is evaluated by:

 - Best F1.
 - Best Recall.
 - Best Precision.
 - Inference time.

Note that this assumes that the foreign key primitive returns a ranking of foreign 
keys. For each threshold - i.e. take the top N predicted foreign keys - the F1 score
is computed and only the threshold that produces the best F1 score is selected. It 
returns the corresponding F1/Recall/Precision values.

We will use leave-one-out validation and report the test performance on each dataset
in the S3 bucket.

## Column Map
Column map detection is evaluated by:

 - F1.
 - Recall.
 - Precision.
 - Inference time.

Note that this assumes that the column map primitive returns a set of columns that it
thinks contributed to the target derived column. Since each dataset can have multiple
derived columns, this will report a F1/recall/precision/time tuple for each derived 
column in the dataset.

We will use leave-one-out validation and report the test performance on each dataset
in the S3 bucket.
