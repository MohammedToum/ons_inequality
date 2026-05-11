select

    *
    /*
    """
    All of these staging tables need their select statements to be 
    hard coded, NO SELECT *'s. 


    They should be transformed so the necessary changes are made to
    the raw data before it is being brought into the staging tables
    """
    */


from {{ source('raw_ons', 'raw_wellbeing_local_authority') }}