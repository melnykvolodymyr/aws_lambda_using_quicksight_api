const AWS = require("aws-sdk");
const fs = require("fs");
const secretsManager = new AWS.SecretsManager(process.env.AWS_REGION);
var quicksight = getQuickSightClient(process.env.DOMAIN);

const ddbTable = "canaryWorkflowTestData";
const credsNotRequired = ["athena", "adobe", "s3"];

const dataVersion = 1;

AWS.config.apiVersions = {
  quicksight: "2018-04-01",
  dynamodb: "2012-08-10",
};

const data = fs.readFileSync("./required_datasets_bkup.json");
var sources = JSON.parse(data);
var { dataSources, dataSets } = sources;
var secretString = "";
var quicksightUserArn = "";

const callback = (err, response) => {
  if (err) {
    console.error(err);
    return;
  }
  console.log("Created ResourceName:" + response);
};

exports.handler = async (event, context, cal) => {
  if (!sources) {
    sources = event;
  }
  // console.log('REQUEST RECEIVED:\n' + JSON.stringify(sources));
  const invokedFunctionArn = context.invokedFunctionArn;
  const accountId = invokedFunctionArn.split(":")[4];
  try {
    await subscribeQuickSight(quicksight, accountId);
    if (process.env.IDENTITY_TYPE === "IAM") {
      // don't create Identity Center Users
      await createQuickSightUser(accountId);
      await describeQuickSightUser(accountId);
    }

    //////////////////////////////////////// Delete All Data Sources ////////////////////////////////////
    
    await deleteAllDataSources(accountId);
    await deleteAllDataSets(accountId);

    //////////////////////////////////////// Create Data Sources ////////////////////////////////////
    // for (let source of dataSources) {
    //   try {
    //     secretString = '';
    //     if (source.secretManagerArn) {
    //       secretString = await getSecretString(source.secretManagerArn);
    //     }
    //     await createDataSource(quicksight, source, accountId, context);
    //     // await writeMetadataToDDB(source, context, context);
    //     // if (source.type === 'INCREMENTAL_REFRESH') {
    //     //   await putDataSetProperties(quicksight, accountId, source, context);
    //     // }
    //   } catch (err) {
    //     console.error(err);
    //   }
    // }

    //////////////////////////////////////// Create Data Sets //////////////////////////////////////
    // for (let dataSet of dataSets) {
    //   try {
    //     let dataSetId = await createDataSet(quicksight, dataSet, accountId, context);
        
    //     if(dataSetId !== "exist") {
    //       let ingStatus = await createIngestion(quicksight, dataSetId, accountId, context);
    //       console.log("Ingestion status: ", ingStatus);
    //     }
    //   } catch (err) {
    //     console.error(err);
    //   }
    // }
  } catch (err) {
    console.log(err);
  }
};

const deleteDataSource = (accountId, dataSourceId) => {
  const params = {
    AwsAccountId: accountId,
    DataSourceId: dataSourceId
  }
  return new Promise((res, rej) => {
    quicksight.deleteDataSource(params, (err, data) => {
      if (err) {
        console.log("Error: ", err);
        rej();
      } else {
        console.log("data source was deleted: " + dataSourceId);
        res();
      }
    })
  })
}

const getDataSourceList = (accountId) => {

  const params = {
    AwsAccountId: accountId
  }

  return new Promise((res, rej) => {
    quicksight.listDataSources(params, (err, data) => {
      if (err) {
        console.log("Error: ", err.message);
        rej();
      } else {
        console.log('Get dataset list succeeded');
        res(data.DataSources);
      }
    });
  });
}

const deleteAllDataSources = async (accountId) => {
  const dataSouces = await getDataSourceList(accountId);
  if (dataSouces.length == 0) {
    console.log('There is not data set');
    return;
  }
  for(let source of dataSouces) {
    await deleteDataSource(accountId, source.DataSourceId);
  }
}

const deleteDataSet = (accountId, dataSetId) => {
  const params = {
    AwsAccountId: accountId,
    DataSetId: dataSetId
  }
  return new Promise((res, rej) => {
    quicksight.deleteDataSet(params, (err, data) => {
      if (err) {
        console.log("Error: ", err);
        rej();
      } else {
        console.log("data set was deleted: " + dataSetId);
        res();
      }
    })
  })
}

const getDataSetList = (accountId) => {

  const params = {
    AwsAccountId: accountId
  }

  return new Promise((res, rej) => {
    quicksight.listDataSets(params, (err, data) => {
      if (err) {
        console.log("Error: ", err.message);
        rej();
      } else {
        console.log('Get dataset list succeeded');
        res(data.DataSetSummaries);
      }
    });
  });
}

const deleteAllDataSets = async (accountId) => {
  const dataSets = await getDataSetList (accountId);
  if (dataSets.length == 0) {
    console.log('There is not data set');
    return;
  }
  for(let source of dataSets) {
    await deleteDataSet(accountId, source.DataSetId);
  }
}

const createDataSource = (quicksight, source, accountId, context, cal) => {
  var params = {};
  var dataSourceId = source.name.replace(/ /g, "-");
  var srcName = `${dataSourceId}${dataVersion}`;
  if (credsNotRequired.includes(source.dataSource.toLowerCase())) {
    params = {
      AwsAccountId: accountId,
      DataSourceId: srcName,
      Name: srcName,
      Type: source.dataSource,
      DataSourceParameters: getDataSourceParameters(source, context),
    };
  } else {
    params = {
      AwsAccountId: accountId,
      DataSourceId: srcName,
      Name: srcName,
      Type: source.dataSource,
      Credentials: dataSourceCredentials(source),
      DataSourceParameters: getDataSourceParameters(source, context),
    };
  }

  params.Permissions = [
    {
      // Principal: process.env.QUICKSIGHT_USER,
      Principal: quicksightUserArn,
      Actions: [
        "quicksight:PassDataSource",
        "quicksight:UpdateDataSource",
        "quicksight:DeleteDataSource",
        "quicksight:DescribeDataSource",
        "quicksight:DescribeDataSourcePermissions",
        "quicksight:UpdateDataSourcePermissions",
      ],
    },
  ];

  console.log("Creating following DataSource: " + srcName);
  return new Promise((res, rej) => {
    quicksight.createDataSource(params, (err, data) => {
      if (err) {
        if (err.code === "ResourceExistsException") {
          console.warn("Warn:", err.message);
          res();
        } else {
          console.error("Error:", err.message);
          rej();
        }
      } else {
        console.log(
          `Created data source ${srcName} with ID ${srcName}`
        );
        res();
      }
    });
  });
};

const createIngestion = (quicksight, dataSetId, accountId, context, val) => {
  console.log("Creating ingestion...");
  const params = {
    AwsAccountId: accountId,
    DataSetId: dataSetId,
    IngestionId: `ingestion-id-${dataSetId}`,
    IngestionType: 'FULL_REFRESH'
  }
  return new Promise((res, rej) => {
    quicksight.createIngestion(params, (err, data) => {
      if (err) {
        if (err.code === "ResourceExistsException") {
          console.warn("Warn:", err.message);
          res();
        } else {
          console.error("Error: ", err.message);
          rej("Error: Creating ingestion");
        }
      } else {
        console.log(`Created ingestion with ID ingestion-${dataSetId}`);
        res(data.IngestionStatus);
      }
    });
  });
}

const createDataSet = (quicksight, source, accountId, context, cal) => {
  const timeString = Date.now().toString();
  const dataSetId = `${source.name}${dataVersion}`;
  const params = {
    AwsAccountId: accountId,
    DataSetId: dataSetId,
    ImportMode: "SPICE",
    Name: dataSetId,
    PhysicalTableMap: getPhysicalTableMap(source, accountId),
    LogicalTableMap: getLogicalTableMap(source, accountId),
  };


  params.Permissions = [
    {
      Principal: quicksightUserArn,
      Actions: [
        "quicksight:PassDataSet",
        "quicksight:DescribeIngestion",
        "quicksight:CreateIngestion",
        "quicksight:UpdateDataSet",
        "quicksight:DeleteDataSet",
        "quicksight:DescribeDataSet",
        "quicksight:CancelIngestion",
        "quicksight:DescribeDataSetPermissions",
        "quicksight:ListIngestions",
        "quicksight:UpdateDataSetPermissions",
      ],
    },
  ];
  
  return new Promise((res, rej) => {
    quicksight.createDataSet(params, (err, data) => {
      if (err) {
        if (err.code === "ResourceExistsException") {
          console.warn("Warn:", err.message);
          res("exist");
        } else {
          console.error("Error:", err.message);
          rej();
        }
      } else {
        console.log(`Created dataset ${source.name}`);
        res(dataSetId);
      }
    });
  });
};

const writeMetadataToDDB = (source, context, cal) => {
  const params = {
    TableName: ddbTable,
    Key: {
      dataSource: { S: source.name },
      type: { S: source.dataSource },
    },
  };
  return new Promise((res, rej) => {
    new AWS.DynamoDB().getItem(params, function (err, data) {
      if (err) {
        console.log("Error", err);
        rej(err);
      } else {
        if (data.Item) {
          console.log("warn: error writing to ddb. item exists ", data.Item);
          res();
        } else {
          new AWS.DynamoDB().putItem(
            {
              TableName: ddbTable,
              Item: {
                dataSource: { S: source.name },
                type: { S: source.dataSource },
                description: { S: source.type },
                pdsId: { S: "" },
                sourceType: { S: source.sourceType },
              },
            },
            function (err, data) {
              if (err) {
                console.log(
                  "Error putting item into dynamodb failed: " + err.message
                );
                rej(err);
              } else {
                console.log(
                  "Success writing data: " +
                    JSON.stringify(source.name, null, "  ")
                );
                res();
              }
            }
          );
        }
      }
    });
  });
};
const getDataSourceParameters = (source, context) => {
  const dataSourceParameters = {};

  const dataSource = source.dataSource.toLowerCase().trim();

  if (dataSource === "amazonelasticsearch") {
    dataSourceParameters.AmazonElasticsearchParameters = {
      Domain: source.domain,
    };
  } else if (dataSource === "amazonopensearch") {
    dataSourceParameters.AmazonOpenSearchParameters = {
      Domain: source.domain,
    };
  } else if (dataSource === "athena") {
    dataSourceParameters.AthenaParameters = {
      // RoleArn: context.invokedFunctionArn,
      RoleArn: context.invokedFunctionArn,
      WorkGroup: source.workGroup,
    };
  } else if (dataSource === "aurora") {
    dataSourceParameters.AuroraParameters = {
      Database: source.database,
      Host: source.host,
      Port: source.port,
    };
  } else if (dataSource === "aurora_postgresql") {
    dataSourceParameters.AuroraPostgreSqlParameters = {
      Database: source.database,
      Host: source.host,
      Port: source.port,
    };
  } else if (dataSource === "awsiotanalytics") {
    dataSourceParameters.AwsIotAnalyticsParameters = {
      DataSetName: source.dataSetName,
    };
  } else if (dataSource === "databricks") {
    dataSourceParameters.DatabricksParameters = {
      Host: source.host,
      Port: source.port,
      SqlEndpointPath: source.sqlEndPointPath,
    };
  } else if (dataSource === "exasol") {
    dataSourceParameters.ExasolParameters = {
      Host: source.host,
      Port: source.port,
    };
  } else if (dataSource === "jira") {
    dataSourceParameters.JiraParameters = {
      SiteBaseUrl: source.siteBaseUrl,
    };
  } else if (dataSource === "mariadb") {
    dataSourceParameters.MariaDbParameters = {
      Database: source.database,
      Host: source.host,
      Port: source.port,
    };
  } else if (dataSource === "mysql") {
    dataSourceParameters.MySqlParameters = {
      Database: source.database,
      Host: source.host,
      Port: source.port,
    };
  } else if (dataSource === "oracle") {
    dataSourceParameters.OracleParameters = {
      Database: source.database,
      Host: source.host,
      Port: source.port,
    };
  } else if (dataSource === "postgresql") {
    dataSourceParameters.PostgreSqlParameters = {
      Database: source.database,
      Host: source.host,
      Port: source.port,
    };
  } else if (dataSource === "presto") {
    dataSourceParameters.PrestoParameters = {
      Catalog: source.catalog,
      Host: source.host,
      Port: source.port,
    };
  } else if (dataSource === "rds") {
    dataSourceParameters.RdsParameters = {
      Database: source.database,
      InstanceId: source.instanceId,
    };
  } else if (dataSource === "redshift") {
    dataSourceParameters.RedshiftParameters = {
      Database: source.database,
      ClusterId: source.clusterId,
      Host: source.host,
      Port: source.port,
    };
  } else if (dataSource === "s3") {
    dataSourceParameters.S3Parameters = {
      //https://s3-us-west-2.amazonaws.com/sn-platform-canaries-s3-data/SampleTestManifestFile.json
      ManifestFileLocation: {
        Bucket: source.bucket,
        Key: source.key,
      },
      RoleArn: source.roleArn,
    };
  } else if (dataSource === "servicenow") {
    dataSourceParameters.ServiceNowParameters = {
      SiteBaseUrl: source.siteBaseUrl,
    };
  } else if (dataSource === "snowflake") {
    dataSourceParameters.SnowflakeParameters = {
      Database: source.database,
      Host: source.host,
      Warehouse: source.warehouse,
    };
  } else if (dataSource === "spark") {
    dataSourceParameters.SparkParameters = {
      Host: source.host,
      Port: source.port,
    };
  } else if (dataSource === "sqlserver") {
    dataSourceParameters.SqlServerParameters = {
      Database: source.database,
      Host: source.host,
      Port: source.port,
    };
  } else if (dataSource === "teradata") {
    dataSourceParameters.TeradataParameters = {
      Database: source.database,
      Host: source.host,
      Port: source.port,
    };
  } else if (dataSource === "twitter") {
    dataSourceParameters.TwitterParameters = {
      MaxRows: source.number.maxRows,
      Query: source.query,
    };
  }

  return dataSourceParameters;
};
const dataSourceCredentials = (source) => {
  const Credentials = {
    CredentialPair: {},
  };
  if (secretString !== "") {
    Credentials.CredentialPair.Password = secretString.password;
    Credentials.CredentialPair.Username = secretString.username;
  } else if (source.username && source.password) {
    Credentials.CredentialPair.Password = source.password;
    Credentials.CredentialPair.Username = source.username;
  }
  return Credentials;
};
function getQuickSightClient(domain) {
  var integEndpoint = `https://quicksight.integ.${process.env.AWS_REGION}.quicksight.aws.a2z.com`;
  // set quicksight client for integ domain when applicable
  if (domain === "integ") {
    return new AWS.QuickSight({
      region: process.env.AWS_REGION,
      endpoint: integEndpoint,
    });
  } else {
    return new AWS.QuickSight({ region: process.env.AWS_REGION });
  }
}
function getSecretString(secretArn) {
  /*
  function takes in SecretsManager ARN
  Returns Javascript Object containing username and password
  Assumption is made that they key values are username and password for your Secret
  */
  // console.log("Getting secret String for: " + secretArn);
  var params = {
    SecretId: secretArn,
  };
  return new Promise((resolve, reject) => {
    secretsManager.getSecretValue(params, function (err, data) {
      if (err) {
        console.log(err, err.stack); // an error occurred
        console.log("There was an Error");
      } else {
        var secretString = JSON.parse(data.SecretString);
        resolve(secretString);
      }
    });
  });
}

function getPhysicalTableMap(source, accountId) {
  const dataSourceArn = `arn:aws:quicksight:${process.env.AWS_REGION}:${accountId}:datasource/${source.sourceName}${dataVersion}`;

  let physicalTableMap = {};

  for (let table of source.tables) {
    let newData = {};
    let { name, alias, type, query, columns, schema } = table;

    if (type == "BASIC" || type == "JOIN") {
      newData["RelationalTable"] = {
        DataSourceArn: dataSourceArn,
        Name: name,
        InputColumns: [...columns]
      };
      if(schema) {
        newData["RelationalTable"]["Schema"] = schema;
      }
    } else if (type == "CUSTOM_SQL") {
      newData["CustomSql"] = {
        DataSourceArn: dataSourceArn,
        Name: alias,
        SqlQuery: query,
        Columns: [...columns],
      };
    }
    physicalTableMap = {
      ...physicalTableMap,
      [alias]: newData,
    };
  }

  return physicalTableMap;
}

function getLogicalTableMap(source, accountId) {
  const dataSourceArn = `arn:aws:quicksight:${process.env.AWS_REGION}:${accountId}:datasource/${source.sourceName}`;

  let logicalTableMap = {};
  for (let table of source.tables) {
    logicalTableMap = {
      ...logicalTableMap,
      [`${table.alias}`]: {
        Alias: `${table.alias}`,
        Source: {
          PhysicalTableId: table.alias,
        },
      },
    };
  }

  if (source.joinInstruction) {
    let logicalTableName = source.joinInstruction.tableName;

    logicalTableMap = {
      ...logicalTableMap,
      [logicalTableName]: {
        // remove hard code value
        Alias: source.joinInstruction.alias,
        Source: {
          JoinInstruction: {
            LeftOperand: source.joinInstruction.leftOperand,
            OnClause: source.joinInstruction.onClause,
            RightOperand: source.joinInstruction.rightOperand,
            Type: source.joinInstruction.type,
          },
        },
      },
    };
  }
  return logicalTableMap;
}

function createQuickSightUser(accountId) {
  var quicksight = new AWS.QuickSight({
    region: process.env.QUICKSIGHT_IAM_REGION,
  });
  var params = {
    AwsAccountId: accountId,
    // 'dataingestioncanary@amazon.com'
    Email: process.env.USER_EMAIL,
    IdentityType: process.env.IDENTITY_TYPE,
    Namespace: "default",
    UserRole: "ADMIN",
    //'arn:aws:iam::accountId:user/quicksightIAMUser
    // IamArn: process.env.QUICKSIGHT_USER,
    IamArn: "arn:aws:iam::190600982480:user/test123",
  };
  return new Promise((resolve, reject) => {
    quicksight.registerUser(params, (err, data) => {
      if (err) {
        if (err.code === "ResourceExistsException") {
          console.warn("Warn:", err.message);
          resolve();
        } else {
          console.error("Error:", err.message);
          reject("error createQuickSightUser");
        }
      } else {
        console.log(`createQuickSightUser Successful`);
        resolve();
      }
    });
  });
}

function describeQuickSightUser(accountId) {
  var quicksight = new AWS.QuickSight({
    region: process.env.QUICKSIGHT_IAM_REGION,
  });
  var params = {
    AwsAccountId: accountId,
    // UserName: 'sn-workflow-canary_us-west-2',
    UserName: "test123",
    Namespace: "default",
  };

  return new Promise((resolve, reject) => {
    quicksight.describeUser(params, (err, data) => {
      if (err) {
        console.error("Error:", err.message);
        reject("error describeQuickSightUser");
      } else {
        quicksightUserArn = data["User"]["Arn"];
        console.log(
          `describeQuickSightUser Successful: ${JSON.stringify(data)}`
        );
        resolve();
      }
    });
  });
}

function getSubscriptionParams(identityType, accountId) {
  /*
  funcstion checks identity type and returns proper parameters.
  Identity Center does rely on the following groups to be created:
  QuickSight_Admin, QuickSight_Author, QuickSight_Reader.
  */

  if (identityType === "IAM_IDENTITY_CENTER") {
    return {
      AwsAccountId: accountId,
      AccountName: process.env.ACCOUNT_NAME,
      AuthenticationMethod: identityType,
      AdminGroup: ["QuickSight_Admin"],
      AuthorGroup: ["QuickSight_Author"],
      ReaderGroup: ["QuickSight_Reader"],
      Edition: "ENTERPRISE",
      EmailAddress: process.env.ACCOUNT_EMAIL_ADDRESS,
      NotificationEmail: process.env.ACCOUNT_EMAIL_ADDRESS,
    };
  } else {
    return {
      AwsAccountId: accountId,
      AccountName: process.env.ACCOUNT_NAME,
      AuthenticationMethod: "IAM_AND_QUICKSIGHT",
      Edition: "ENTERPRISE",
      EmailAddress: process.env.ACCOUNT_EMAIL_ADDRESS,
      NotificationEmail: process.env.ACCOUNT_EMAIL_ADDRESS,
    };
  }
}

function subscribeQuickSight(quicksight, accountId) {
  var params = getSubscriptionParams(process.env.IDENTITY_TYPE, accountId);
  return new Promise((resolve, reject) => {
    quicksight.createAccountSubscription(params, (err, data) => {
      if (err) {
        if (err.code === "ResourceExistsException") {
          console.warn("Warn:", err.message);
          resolve();
        } else {
          console.error("Error:", err.message);
          reject("Error subscribe to  QuickSight");
        }
      } else {
        console.log(`Subscribe to QuickSight Successful`);
        resolve();
      }
    });
  });
}

function putDataSetProperties(quicksight, accountId, source) {
  var params = {
    AwsAccountId: accountId,
    DataSetId: source.dataSetname,
    DataSetRefreshProperties: {
      RefreshConfiguration: {
        IncrementalRefresh: {
          LookbackWindow: {
            Size: source.Size,
            ColumnName: source.ColumnName,
            SizeUnit: source.SizeUnit,
          },
        },
      },
    },
  };

  return new Promise((resolve, reject) => {
    quicksight.putDataSetRefreshProperties(params, (err, data) => {
      if (err) {
        if (err.code === 'ResourceExistsException') {
          console.warn('Warn:', err.message);
          resolve();
        } else {
          console.error('Error:', err.message);
          reject('error while updating DataSetProperties');
        }
      } else {
        console.log(`updated DataSetProperties`);
        resolve();
      }
    });
  });
}
