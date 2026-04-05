package com.github.zly2006.sl.access;

import com.github.zly2006.sl.mixinhelper.RecordMixinHelper;

public interface OperationCarrierAccess {
    RecordMixinHelper.OperationContext sl$getOperationContext();

    void sl$setOperationContext(RecordMixinHelper.OperationContext operationContext);
}
