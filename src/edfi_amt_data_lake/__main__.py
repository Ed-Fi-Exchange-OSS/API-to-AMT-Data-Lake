# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import logging
import sys


def main():
    log_format = (
        '[%(asctime)s] %(levelname)-8s %(name)-12s %(message)s')

    log_level = logging.INFO if sys.argv[-1] == 'debug' else logging.WARNING

    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.StreamHandler()
        ]
    )

    logger = logging.getLogger("edfi_amt_data_lake")
    logger.debug('debug')
    logger.info('info')
    logger.warning('warning')
    logger.error('error')
    logger.critical('critical')


if __name__ == "__main__":
    main()
