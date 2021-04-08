package com.exactpro.cradle.cassandra.dao.healing;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.exactpro.cradle.healing.RecoveryState;

import static com.exactpro.cradle.cassandra.StorageConstants.RECOVERY_STATE_ID;

// TODO: add more properties if necessary

@Entity
public class RecoveryStateEntity
{
    @PartitionKey(0)
    @CqlName(RECOVERY_STATE_ID)
    private String recoverStateId;

    public RecoveryStateEntity()
    {
    }

    public RecoveryStateEntity(RecoveryState state) {
        this.recoverStateId = state.getId();
    }

    public String getRecoverStateId() { return recoverStateId; }

    public void setRecoverStateId(String recoverStateId) { this.recoverStateId = recoverStateId; }
}
