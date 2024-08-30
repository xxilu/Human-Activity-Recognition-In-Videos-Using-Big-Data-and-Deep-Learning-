# Ultralytics YOLO 🚀, AGPL-3.0 license

from ultralytics.engine.results import Results
from ultralytics.models.yolo.detect.predict import DetectionPredictor
from ultralytics.utils import DEFAULT_CFG, LOGGER, ops
from keras.models import load_model
import numpy as np
import tensorflow as tf
import cv2


class PosePredictor(DetectionPredictor):
    """
    A class extending the DetectionPredictor class for prediction based on a pose model.

    Example:
        ```python
        from ultralytics.utils import ASSETS
        from ultralytics.models.yolo.pose import PosePredictor

        args = dict(model='yolov8n-pose.pt', source=ASSETS)
        predictor = PosePredictor(overrides=args)
        predictor.predict_cli()
        ```
    """

    def __init__(self, cfg=DEFAULT_CFG, overrides=None, _callbacks=None):
        """Initializes PosePredictor, sets task to 'pose' and logs a warning for using 'mps' as device."""
        super().__init__(cfg, overrides, _callbacks)
        self.args.task = "pose"
        if isinstance(self.args.device, str) and self.args.device.lower() == "mps":
            LOGGER.warning(
                "WARNING ⚠️ Apple MPS known Pose bug. Recommend 'device=cpu' for Pose models. "
                "See https://github.com/ultralytics/ultralytics/issues/4031."
            )

    # def move_net(model, keypoint_arr):
    #   model = load_model(model)
    #   pred = model.predict(keypoint_arr)
    #   itemindex = np.where(pred == np.max(pred))
    #   print('itemindex:{}'.format(itemindex))
    #   prediction = itemindex[1][0]
    #   print("probability: " + str(np.max(pred) * 100) + "%\nPredicted class : ", class_names[prediction])

    def postprocess(self, preds, img, orig_imgs):
        """Return detection results for a given input image or list of images."""
        preds = ops.non_max_suppression(
            preds,
            self.args.conf,
            self.args.iou,
            agnostic=self.args.agnostic_nms,
            max_det=self.args.max_det,
            classes=self.args.classes,
            nc=len(self.model.names),
        )

        if not isinstance(orig_imgs, list):  # input images are a torch.Tensor, not a list
            orig_imgs = ops.convert_torch2numpy_batch(orig_imgs)

        results = []
        path_model = 'movenet.hdf5'
        print('preds', preds)
        print('len(preds)', len(preds))
        movenet_pred_list = []
        for i, pred in enumerate(preds):

            print('i, predđ', i)
            print('aaaaaaaaaaaaaa', len(pred))
            orig_img = orig_imgs[i]


            pred[:, :4] = ops.scale_boxes(img.shape[2:], pred[:, :4], orig_img.shape).round()

                    # cropped_image = orig_img[int(y1):int(y2), int(x1):int(x2)]

            pred_kpts = pred[:, 6:].view(len(pred), *self.model.kpt_shape) if len(pred) else pred[:, 6:]
            pred_kpts = ops.scale_coords(img.shape[2:], pred_kpts, orig_img.shape)
            img_path = self.batch[0][i]
            print('pred_kptsssssssss', pred[:, :4].shape)

            if  len(pred) != 0:
                for j in preds:
                    for i in pred_kpts:
                        keypoints_arr = tf.reshape(i, [1, -1])
                        keypoints_arr_numpy = keypoints_arr.numpy()

                        movenet = MoveNet(path_model, orig_img)
                        movenet_pred = movenet.predict(keypoints_arr_numpy, orig_img)
                        movenet_pred_list.append(movenet_pred)
                    for box in pred[:, :4]:
                        x1, y1, x2, y2 = box
                        startPoint = (int(x1), int(y1))
                        endPoint = (int(x2), int(y2))
                        cv2.putText(orig_img, movenet_pred, (int(x2 + 10), int(y1 + 60)), cv2.FONT_HERSHEY_COMPLEX, 0.5,
                                    (0, 255, 0), 2)
                    print('movenet_preddddd', movenet_pred)


                results.append(
                        Results(orig_img, path=img_path, names=self.model.names, boxes=pred[:, :6], keypoints=pred_kpts, label_detect=(movenet_pred_list))
                    )
            else:
                # None

                results.append(
                    Results(orig_img, path=img_path, names=self.model.names, boxes=pred[:, :6], keypoints=pred_kpts,
                           label_detect=None)
                )



            # cropped_image = orig_img[int(y1):int(y2), int(x1):int(x2)]

            # print('ket qua',results[0].keypoints)
        print('preddddd', preds)
        print('het')
        return results


class MoveNet():
    def __init__(self, path_model, org_img):
        self.model = load_model(path_model)
        self.org_img = org_img
        # self.bbox = bbox

    def predict(self, keypoints_arr, org_img):
        labels = ['boxing', 'handclapping', 'handwaving', 'jogging', 'running', 'walking']
        pred = self.model.predict(keypoints_arr)  # Dự đoán
        itemindex = np.where(pred == np.max(pred))
        prediction = itemindex[1][0]
        # print('shapeeeeeimg', img.shape)
        # plt.imsave('blaaa1.jpg', img)

        # print('startPointttttttt', startPoint)
        # cv2.rectangle(org_img, (startPoint), (endPoint), (0, 255, 0), 5)

        # cv2.putText(org_img, labels[prediction], (int(x1 + 10), int(y1 + 60)), cv2.FONT_HERSHEY_COMPLEX, 0.5, (0, 255, 0), 2)
        # x1, y1, x2, y2 = bbox
        # startPoint = (int(x1), int(y1))
        # endPoint = (int(x2), int(y2))
        # # print('startPointttttttt', startPoint)
        # cv2.rectangle(org_img, (startPoint), (endPoint), (0, 255, 0), 5)
        # cv2.putText(org_img, labels[prediction], (int(x1 + 10), int(y1 + 60)), cv2.FONT_HERSHEY_COMPLEX, 0.5, (0, 255, 0), 2)

        print('nhãn', labels[prediction])

        itemindex = np.where(pred == np.max(pred))
        print('itemindex:{}'.format(itemindex))
        prediction = itemindex[1][0]
        print("probability: " + str(np.max(pred) * 100) + "%\nPredicted class : ", labels[prediction])
        # cv2.putText(org_img, labels[prediction], (int(x1 - 10), int(y1 - 60)), cv2.FONT_HERSHEY_COMPLEX, 0.5, (0, 255, 0), 2)
        # (int(x1 + 10), int(y1 + 60))
        # draw bbox
        # print('bbox', bbox)
        # for box in bbox[:, :4]:
        #   x1, y1, x2, y2 = box
        #   startPoint = (int(x1), int(y1))
        #   endPoint = (int(x2), int(y2))
        #   # print('startPointttttttt', startPoint)
        #   cv2.rectangle(org_img, (startPoint), (endPoint), (0, 255, 0), 5)

        #   cv2.putText(org_img, labels[prediction], (int(x1 + 10), int(y1 + 60)), cv2.FONT_HERSHEY_COMPLEX, 0.5, (0, 255, 0), 2)
        #   print('nhãn', labels[prediction])
        return labels[prediction]


# if __name__ == "__main__":
#     from ultralytics.utils import ASSETS
#     from ultralytics.models.yolo.pose import PosePredictor
#
#     # path_img = '/content/drive/MyDrive/keypoints_kth/train/boxing/img0001.png'
#     path_img = '/content/drive/MyDrive/img0050.png'
#     args = dict(model='yolov8n-pose.pt', source=path_img)
#     predictor = PosePredictor(overrides=args)
#     predictor.predict_cli()

















