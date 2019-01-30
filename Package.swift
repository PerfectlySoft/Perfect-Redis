// swift-tools-version:4.0
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
    products: [
        .library(name: "PerfectRedis", targets: ["PerfectRedis"])
    ],
    dependencies: [
        .package(url: "https://github.com/PerfectlySoft/Perfect-Net.git", from: "3.2.1")
    ],
    targets: [
        .target(name: "PerfectRedis", dependencies: ["PerfectNet"], path: "Sources"),
        .testTarget(name: "PerfectRedisTests", dependencies: ["PerfectNet", "PerfectRedis"])
    ]
)
