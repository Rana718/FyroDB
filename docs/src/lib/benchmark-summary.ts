export const normalBenchmark = {
  "checkpoint": "d5a178b",
  "configurations": [
    {
      "workers": 1,
      "get_mops": 20.130240481851608,
      "set_mops": 17.382377484275487,
      "rss_mib": 782.33203125,
      "incr_mops": 13.96027153817043,
      "list_mops": 9.922929450407688
    },
    {
      "workers": 2,
      "get_mops": 32.96209944629804,
      "set_mops": 27.605481843581973,
      "rss_mib": 794.62890625,
      "incr_mops": 27.31297393301662,
      "list_mops": 11.811475922521891
    },
    {
      "workers": 3,
      "get_mops": 44.616481034517705,
      "set_mops": 32.25088806932287,
      "rss_mib": 797.1328125,
      "incr_mops": 30.385816921624432,
      "list_mops": 21.438579263637113
    },
    {
      "workers": 6,
      "get_mops": 47.418887302247654,
      "set_mops": 37.703658100151735,
      "rss_mib": 1296.5390625,
      "incr_mops": 45.85788535854252,
      "list_mops": 30.177440483184274
    },
    {
      "workers": 12,
      "get_mops": 55.897014580138155,
      "set_mops": 43.34713371694767,
      "rss_mib": 1596.83984375,
      "incr_mops": 73.88667011323354,
      "list_mops": 38.50473485023507
    }
  ]
} as const;
