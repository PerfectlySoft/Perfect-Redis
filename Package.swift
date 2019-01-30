//
//  Package.swift
//  Perfect-Redis
//
//  Created by Kyle Jessup on 2016-06-03.
//	Copyright (C) 2016-2019 PerfectlySoft, Inc.
//
//===----------------------------------------------------------------------===//
//
// This source file is part of the Perfect.org open source project
//
// Copyright (c) 2015 - 2016 PerfectlySoft Inc. and the Perfect project authors
// Licensed under Apache License v2.0
//
// See http://perfect.org/licensing.html for license information
//
//===----------------------------------------------------------------------===//
//

import PackageDescription

let package = Package(
    name: "PerfectRedis",
    targets: [],
    dependencies: [
        .package(url: "https://github.com/PerfectlySoft/Perfect-Net.git", from: "3.2.2")
    ],
    exclude: []
)
