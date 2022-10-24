# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.
from edfi_amt_data_lake.parquet.amt.epp.candidate_dim.main import candidate_dim
from edfi_amt_data_lake.parquet.amt.epp.candidate_survey_dim.main import (
    candidate_survey_dim,
)
from edfi_amt_data_lake.parquet.amt.epp.epp_dim.main import epp_dim
from edfi_amt_data_lake.parquet.amt.epp.evaluation_element_rating_dim.main import (
    evaluation_element_rating_dim,
)
from edfi_amt_data_lake.parquet.amt.epp.race_descriptor_dim.main import (
    race_descriptor_dim,
)
from edfi_amt_data_lake.parquet.amt.epp.sex_descriptor_dim.main import (
    sex_descriptor_dim,
)
from edfi_amt_data_lake.parquet.amt.epp.term_descriptor_dim.main import (
    term_descriptor_dim,
)


def epp_collection(school_year) -> None:
    candidate_survey_dim(school_year)
    evaluation_element_rating_dim(school_year)
    term_descriptor_dim(school_year)
    race_descriptor_dim(school_year)
    sex_descriptor_dim(school_year)
    epp_dim(school_year)
    candidate_dim(school_year)
