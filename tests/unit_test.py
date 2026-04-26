def test_api_key(api_key):
    assert api_key=="MOCK_KEY1234"
    
def test_channel_handle(channel_handle):
    assert channel_handle=="MOCK_CHANNEL"
    
def test_postgres_conn(mock_postgres_conn_vars):
    conn= mock_postgres_conn_vars
    assert conn.login=="mock_username"
    assert conn.password=="mock_password"
    assert conn.host=="mock_host"
    assert conn.port==1234
    assert conn.schema=="mock_db"
    
    
def test_dags_integrity(dagbag):
    #1.No import errors check
    assert dagbag.import_errors== {}, f"Import errors found: {dagbag.import_errors}"
    print("-------------------")
    print(dagbag.import_errors)
    
    #2. All the dags that we defined are being loaded 
    
    expected_dag_ids=["produce_json","update_db","data_quality_checks"]
    loaded_dags_ids=list(dagbag.dags.keys()) #the dag.keys() method returns a view object that displays a list of all the keys in the dictionary, which in this case are the DAG IDs.
    print("-------------------")
    print(dagbag.dags.keys())
   
    for dag_id in expected_dag_ids:
        assert dag_id in loaded_dags_ids,f"DAG {dag_id} is missing from the loaded DAGs"
    
    #3. number of dags is correct
    assert dagbag.size()==3, f"Expected 3 DAGs, but found {dagbag.size()}"
    print("-------------------")
    
    
    #4. ensure each dag has the expected tasks
    expected_task_counts={
        "produce_json":4,
        "update_db":2,
        "data_quality_checks":2
    }
    print("-------------------")
    for dag_id, dag in dagbag.dags.items():
        expected_count=expected_task_counts[dag_id]
        actual_count= len(dag.tasks)
        assert (
            expected_count == actual_count
        ), f"DAG {dag_id} has {actual_count} tasks, expected {expected_count}"
        print(dag_id, len(dag.tasks))