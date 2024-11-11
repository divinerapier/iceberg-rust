// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;

use opendal::raw::HttpClient;
use opendal::services::OssConfig;
use opendal::{Configurator, Operator};
use url::Url;

use crate::{Error, ErrorKind, Result};

/// The domain name used to access OSS. OSS uses HTTP Restful APIs to provide services. Different
/// regions are accessed by using different endpoints. For the same region, access over the
/// internal network or over the Internet also uses different endpoints. For more information, see:
/// https://www.alibabacloud.com/help/doc-detail/31837.htm
///
/// https://github.com/apache/iceberg/blob/1e82c476df58ba467342eec98cb02ec0e2c75bd1/aliyun/src/main/java/org/apache/iceberg/aliyun/AliyunProperties.java#L33
pub const OSS_ENDPOINT: &str = "oss.endpoint";

/// Aliyun uses an AccessKey pair, which includes an AccessKey ID and an AccessKey secret to
/// implement symmetric encryption and verify the identity of a requester. The AccessKey ID is used
/// to identify a user.
///
/// For more information about how to obtain an AccessKey pair, see:
/// https://www.alibabacloud.com/help/doc-detail/53045.htm
///
/// https://github.com/apache/iceberg/blob/1e82c476df58ba467342eec98cb02ec0e2c75bd1/aliyun/src/main/java/org/apache/iceberg/aliyun/AliyunProperties.java#L43
pub const OSS_ACCESS_KEY_ID: &str = "client.access-key-id";

/// Aliyun uses an AccessKey pair, which includes an AccessKey ID and an AccessKey secret to
/// implement symmetric encryption and verify the identity of a requester. The AccessKey secret is
/// used to encrypt and verify the signature string.
///
/// For more information about how to obtain an AccessKey pair, see:
/// https://www.alibabacloud.com/help/doc-detail/53045.htm
///
/// https://github.com/apache/iceberg/blob/1e82c476df58ba467342eec98cb02ec0e2c75bd1/aliyun/src/main/java/org/apache/iceberg/aliyun/AliyunProperties.java#L53
pub const OSS_ACCESS_KEY_SECRET: &str = "client.access-key-secret";

/// Parse iceberg props to oss config.
pub(crate) fn oss_config_parse(mut m: HashMap<String, String>) -> Result<OssConfig> {
    let mut cfg = OssConfig::default();
    if let Some(endpoint) = m.remove(OSS_ENDPOINT) {
        cfg.endpoint = Some(endpoint);
    };
    if let Some(access_key_id) = m.remove(OSS_ACCESS_KEY_ID) {
        cfg.access_key_id = Some(access_key_id);
    };
    if let Some(secret_access_key) = m.remove(OSS_ACCESS_KEY_SECRET) {
        cfg.access_key_secret = Some(secret_access_key);
    };

    Ok(cfg)
}

/// Build new opendal operator from give path.
pub(crate) fn oss_config_build(
    client: &reqwest::Client,
    cfg: &OssConfig,
    path: &str,
) -> Result<Operator> {
    let url = Url::parse(path)?;
    let bucket = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid oss url: {}, missing bucket", path),
        )
    })?;

    let builder = cfg
        .clone()
        .into_builder()
        // Set bucket name.
        .bucket(bucket)
        // Set http client we want to use.
        .http_client(HttpClient::with(client.clone()));

    Ok(Operator::new(builder)?.finish())
}
