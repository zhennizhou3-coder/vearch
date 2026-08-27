/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include <cstdlib>
#include <cstring>
#include <string>

#include "c_api/gamma_api.h"
#include "monitor/prometheus_client.h"

void GetEngineMetrics(void * /*engine*/, char **metrics_str, int *len) {
  std::string metrics = vearch::monitor::GetEngineMetricsText();
  *len = static_cast<int>(metrics.length());
  *metrics_str = static_cast<char *>(malloc(*len * sizeof(char)));
  if (*metrics_str != nullptr && *len > 0) {
    memcpy(*metrics_str, metrics.c_str(), *len);
  } else {
    *len = 0;
  }
}
