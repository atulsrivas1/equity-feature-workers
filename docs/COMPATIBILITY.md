# Compatibility

Only CPython>=3.12,<3.13 x64 on native Windows/Linux CI is qualified, with exact patch/OS in each artifact report. Initial version0.1.0a0 foundations use canonical contracts/features0.0.4a4. I/O contracts depends on canonical contracts; SDK depends on matching I/O contracts; workers depends on SDK. Backend dependencies stay optional under later stories. Other platforms/versions are unqualified. Core package/module and metadata fingerprints must remain equal before/after companion installation/import. No mathematics/input protocol changes.
