# coding=utf-8


with description('Importing public models'):
    with it('exposes resolution 4131 and 4771 models'):
        from libcnmc.models import F1Res4131, F8Res4131
        from libcnmc.models import F1Res4771, F8Res4771

        assert F1Res4131.__name__ == 'F1Res4131'
        assert F8Res4131.__name__ == 'F8Res4131'
        assert F1Res4771.__name__ == 'F1Res4771'
        assert F8Res4771.__name__ == 'F8Res4771'
