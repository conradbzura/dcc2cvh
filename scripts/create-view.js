// Create the 'files' view with aggregation pipeline
db.files.drop();

db.createView("files", "file", [
  // DCC lookup
  {
    $lookup: {
      from: "dcc",
      localField: "submission",
      foreignField: "submission",
      as: "dcc",
    },
  },
  {
    $unwind: {
      path: "$dcc",
      preserveNullAndEmptyArrays: true,
    },
  },
  // File format lookup
  {
    $lookup: {
      from: "file_format",
      localField: "file_format",
      foreignField: "id",
      as: "file_format",
    },
  },
  {
    $unwind: {
      path: "$file_format",
      preserveNullAndEmptyArrays: true,
    },
  },
  // Data type lookup
  {
    $lookup: {
      from: "data_type",
      localField: "data_type",
      foreignField: "id",
      as: "data_type",
    },
  },
  {
    $unwind: {
      path: "$data_type",
      preserveNullAndEmptyArrays: true,
    },
  },
  // Assay type lookup
  {
    $lookup: {
      from: "assay_type",
      localField: "assay_type",
      foreignField: "id",
      as: "assay_type",
    },
  },
  {
    $unwind: {
      path: "$assay_type",
      preserveNullAndEmptyArrays: true,
    },
  },
  // Collection-file cross reference
  {
    $lookup: {
      from: "file_in_collection",
      let: { id_namespace: "$id_namespace", local_id: "$local_id" },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$file_id_namespace", "$$id_namespace"] },
                { $eq: ["$file_local_id", "$$local_id"] },
              ],
            },
          },
        },
        // Collection lookup
        {
          $lookup: {
            from: "collection",
            let: {
              collection_id_namespace: "$collection_id_namespace",
              collection_local_id: "$collection_local_id",
            },
            pipeline: [
              {
                $match: {
                  $expr: {
                    $and: [
                      { $eq: ["$id_namespace", "$$collection_id_namespace"] },
                      { $eq: ["$local_id", "$$collection_local_id"] },
                    ],
                  },
                },
              },
              // Biosample-collection cross reference
              {
                $lookup: {
                  from: "biosample_in_collection",
                  let: {
                    collection_id_namespace: "$id_namespace",
                    collection_local_id: "$local_id",
                  },
                  pipeline: [
                    {
                      $match: {
                        $expr: {
                          $and: [
                            {
                              $eq: [
                                "$collection_id_namespace",
                                "$$collection_id_namespace",
                              ],
                            },
                            {
                              $eq: [
                                "$collection_local_id",
                                "$$collection_local_id",
                              ],
                            },
                          ],
                        },
                      },
                    },
                    // Biosample lookup
                    {
                      $lookup: {
                        from: "biosample",
                        let: {
                          biosample_id_namespace: "$biosample_id_namespace",
                          biosample_local_id: "$biosample_local_id",
                        },
                        pipeline: [
                          {
                            $match: {
                              $expr: {
                                $and: [
                                  {
                                    $eq: [
                                      "$id_namespace",
                                      "$$biosample_id_namespace",
                                    ],
                                  },
                                  {
                                    $eq: ["$local_id", "$$biosample_local_id"],
                                  },
                                ],
                              },
                            },
                          },
                          // Anatomy lookup
                          {
                            $lookup: {
                              from: "anatomy",
                              let: { anatomy_ref: "$anatomy" },
                              pipeline: [
                                {
                                  $match: {
                                    $expr: { $eq: ["$id", "$$anatomy_ref"] },
                                  },
                                },
                              ],
                              as: "anatomy",
                            },
                          },
                          { $unwind: "$anatomy" },
                        ],
                        as: "biosample",
                      },
                    },
                    { $unwind: "$biosample" },
                    { $replaceRoot: { newRoot: "$biosample" } },
                  ],
                  as: "biosamples",
                },
              },
            ],
            as: "collection",
          },
        },
        { $unwind: "$collection" },
        { $replaceRoot: { newRoot: "$collection" } },
      ],
      as: "collections",
    },
  },
]);

print("Created 'files' view successfully");
