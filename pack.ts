import * as coda from "@codahq/packs-sdk";

export const pack = coda.newPack();

pack.setUserAuthentication({
  type: coda.AuthenticationType.AWSAccessKey,
  service: "s3",
});

pack.addNetworkDomain("amazonaws.com");

function getBaseUrl(region: string, bucket: string) {
  return `https://${bucket}.s3.${region}.amazonaws.com/`
}

pack.addFormula({
  name: "CreateBucket",
  description: "Creates a bucket.",
  parameters: [
    coda.makeParameter({
      type: coda.ParameterType.String,
      name: "bucket",
      description: "The name of the bucket to create.",
    }),
    coda.makeParameter({
      type: coda.ParameterType.String,
      name: "region",
      description: "Region to create the bucket in (i.e. us-east-1).",
    }),
  ],
  resultType: coda.ValueType.String,
  execute: async function (args, context) {
    const [bucket, region] = args;
    await context.fetcher.fetch({
      method: "PUT",
      url: getBaseUrl(region, bucket),
    });
    return "";
  },
  isAction: true,
});

const ObjectSchema = coda.makeObjectSchema({
  properties: {
    key: { type: coda.ValueType.String },
    lastModified: { type: coda.ValueType.String, codaType: coda.ValueHintType.DateTime },
    etag: { type: coda.ValueType.String },
    size: { type: coda.ValueType.Number },
    storageClass: { type: coda.ValueType.String },
    bucket: { type: coda.ValueType.String },
    region: { type: coda.ValueType.String },
  },
  displayProperty: "key",
  idProperty: "key",
});

pack.addSyncTable({
  name: "Objects",
  description: "List objects in an S3 bucket.",
  identityName: "S3",
  schema: ObjectSchema,
  formula: {
    name: "Objects",
    description: "List objects in an S3 bucket.",
    parameters: [
      coda.makeParameter({
        type: coda.ParameterType.String,
        name: "bucket",
        description: "Bucket to list.",
      }),
      coda.makeParameter({
        type: coda.ParameterType.String,
        name: "region",
        description: "Region the bucket is in (i.e. us-east-1).",
      }),
    ],
    execute: async function (args, context) {
      const [bucket, region] = args;
      const response = await context.fetcher.fetch({
        method: "GET",
        url: coda.withQueryParams(getBaseUrl(region, bucket), {
          'list-type': '2',
          'continuation-token': context.sync.continuation?.continuationToken,
        }),
      });
      const data = response.body;
      const result = data.Contents.map((item) => {
        return {
          ...item,
          etag: item.ETag[0].replace(/"/g, ""),
          bucket,
          region,
        }
      });
      return {
        result,
        continuation: data.IsTruncated ? { continuationToken: data.NextContinuationToken } : undefined,
      };
    },
  },
});

pack.addFormula({
  name: "PutObject",
  description: "Adds an object to a bucket.",
  parameters: [
    coda.makeParameter({
      type: coda.ParameterType.String,
      name: "bucket",
      description: "Bucket to add object to.",
    }),
    coda.makeParameter({
      type: coda.ParameterType.String,
      name: "region",
      description: "Region the bucket is in (i.e. us-east-1).",
    }),
    coda.makeParameter({
      type: coda.ParameterType.String,
      name: "key",
      description: "Object key for which the PUT action was initiated.",
    }),
    coda.makeParameter({
      type: coda.ParameterType.String,
      name: "contents",
      description: "Contents of object.",
    }),
  ],
  resultType: coda.ValueType.String,
  execute: async function (args, context) {
    const [bucket, region, key, contents] = args;
    await context.fetcher.fetch({
      method: "PUT",
      url: `${getBaseUrl(region, bucket)}/${key}`,
      body: contents,
    });
    return '';
  },
  isAction: true,
});
