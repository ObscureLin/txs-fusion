// SPDX-License-Identifier: GPL-3.0
pragma solidity ^0.8.18;

contract Model {
    // Model parameter updates received in a round
    int32[134302035] update1;
    int32[134302035] update2;
    int32[134302035] update3;

    // Aggregated updates
    int32[134302035] aggregated;

    // Receive local updates uploaded by the nodes
    function SetUpdates(
        uint32 target,
        uint32 index,
        uint32 start,
        int32[240] memory vs
    ) public {
        if (target == 1) {
            for (uint32 i = 0; i < 240 - start; i++) {
                update1[index + i] = vs[start + i];
            }
        } else if (target == 2) {
            for (uint32 i = 0; i < 240 - start; i++) {
                update2[index + i] = vs[start + i];
            }
        } else if (target == 3) {
            for (uint32 i = 0; i < 240 - start; i++) {
                update3[index + i] = vs[start + i];
            }
        }
    }

    // Aggregate a batch of 3 updates
    function Aggregate(uint256 start) public {
        for (uint256 i = start; i < start + 240; i++) {
            aggregated[i] = (update1[i] + update2[i] + update3[i]) / 3;
        }
    }

    // Download aggregated updates
    function download(uint32 start) public view returns (int32[240] memory) {
        int32[240] memory vs;
        for (uint256 i = 0; i < 240; i++) {
            vs[i] = aggregated[start + i];
        }
        return vs;
    }
}
