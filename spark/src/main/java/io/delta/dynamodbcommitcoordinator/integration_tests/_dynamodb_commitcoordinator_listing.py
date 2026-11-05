#
# Copyright (2024) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os


def _is_matching_delta_file(file_name, version):
    expected_prefix = f"{version:020}."
    return (
        file_name.startswith(expected_prefix) and
        file_name.endswith(".json") and
        ".tmp" not in file_name
    )


def _find_matching_delta_files(items, version):
    matching_files = []
    for item in items:
        file_name = os.path.basename(item["Key"])
        if _is_matching_delta_file(file_name, version):
            matching_files.append(file_name)
    return matching_files


def validate_delta_file_listing_response(response, listing_prefix, version, should_exist):
    if 'Contents' not in response:
        assert not should_exist, (
            f"Listing for prefix {listing_prefix} did not return any files even though it "
            f"should have."
        )
        return

    expected_count = 1 if should_exist else 0
    matching_files = _find_matching_delta_files(response['Contents'], version)
    assert len(matching_files) == expected_count, (
        f"Expected {expected_count} matching files for version {version} under prefix "
        f"{listing_prefix}, but found {len(matching_files)}: {matching_files}"
    )


def test_should_fail_if_should_exist_is_true_and_s3_listing_lacks_contents():
    test_name = "it('should fail if should_exist is true and S3 listing lacks Contents')"
    try:
        validate_delta_file_listing_response(
            response={},
            listing_prefix="tables/test/_delta_log/00000000000000000001.",
            version=1,
            should_exist=True
        )
    except AssertionError as error:
        assert "did not return any files" in str(error), (
            f"{test_name}: unexpected assertion message: {error}"
        )
        return

    raise AssertionError(f"{test_name}: expected missing Contents to fail")


def test_should_match_delta_json_files_if_version_has_uuid_suffix():
    test_name = "it('should match Delta JSON files if version has UUID suffix')"
    response = {
        'Contents': [
            {'Key': 'tables/test/_delta_log/_staged_commits/00000000000000000001.uuid.json'},
            {'Key': 'tables/test/_delta_log/_staged_commits/00000000000000000001.uuid.json.tmp'}
        ]
    }

    try:
        validate_delta_file_listing_response(
            response=response,
            listing_prefix="tables/test/_delta_log/_staged_commits/00000000000000000001.",
            version=1,
            should_exist=True
        )
    except AssertionError as error:
        raise AssertionError(f"{test_name}: unexpected assertion: {error}")


def test_should_fail_if_should_exist_is_true_and_matching_file_has_non_json_suffix():
    test_name = (
        "it('should fail if should_exist is true and matching file has non-json suffix')"
    )
    try:
        validate_delta_file_listing_response(
            response={
                'Contents': [
                    {'Key': 'tables/test/_delta_log/00000000000000000001.json.backup'}
                ]
            },
            listing_prefix="tables/test/_delta_log/00000000000000000001.",
            version=1,
            should_exist=True
        )
    except AssertionError as error:
        assert "Expected 1 matching files for version 1" in str(error), (
            f"{test_name}: unexpected assertion message: {error}"
        )
        return

    raise AssertionError(f"{test_name}: expected non-json suffix to fail")


def run_listing_validation_self_tests():
    test_should_fail_if_should_exist_is_true_and_s3_listing_lacks_contents()
    test_should_match_delta_json_files_if_version_has_uuid_suffix()
    test_should_fail_if_should_exist_is_true_and_matching_file_has_non_json_suffix()


if __name__ == "__main__":
    run_listing_validation_self_tests()
