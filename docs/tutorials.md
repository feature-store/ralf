# Tutorials 

(feast)=
## Using `ralf` with Feast 
To get started, follow the Quickstart documentation for Feast. Once you have Feast setup up, you can write to your existing feature views from `ralf` to maintain your features with new data in real-time. 

To synchronize features derived with `ralf` into Feast, specify your feature view's name. 
```python
table = parent_table
          .map(Operator, args=(...))
          .sync(repo_path=".", feature_view="feast_feature_view")
```
